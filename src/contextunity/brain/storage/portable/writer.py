"""Portable Archive writer.

Exports store data into a validated JSONL archive.
"""

from __future__ import annotations

import sqlite3
import struct
from pathlib import Path
from typing import TextIO

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import (
    as_float,
    as_int,
    as_json_dict,
    as_json_dict_list,
    as_str,
    as_str_list,
)
from contextunity.core.tenant_policy import (
    is_production_export_tenant,
    validate_tenant_id,
)
from pydantic import BaseModel

from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION
from contextunity.brain.storage.contracts import BrainStorageProtocol

from ..sqlite.codecs import fetchone_row, sqlite_cell
from .models import (
    BlackboardRecord,
    CellEdgeRecord,
    CellRecord,
    EmbeddingRecord,
    EpisodeRecord,
    PortableManifest,
    SynapseRecord,
    TaxonomyRecord,
    TraceRecord,
)
from .sqlite_export import is_sqlite_export_store

logger = get_contextunit_logger(__name__)


def _row_dict(row: sqlite3.Row) -> dict[str, object]:
    return {key: row[key] for key in row.keys()}


class BrainPortableArchiveWriter:
    """Writes a portable archive from a storage backend."""

    output_dir: Path
    vector_dim: int

    def __init__(
        self,
        output_dir: Path,
        vector_dim: int = DEFAULT_EMBEDDING_DIMENSION,
    ) -> None:
        self.output_dir = output_dir
        self.vector_dim = vector_dim
        self._counts: dict[str, int] = {}
        self._tenants: set[str] = set()
        self._emb_count: int = 0

    async def export(
        self,
        store: BrainStorageProtocol,
        tenant_ids: list[str],
    ) -> PortableManifest:
        """Export selected tenants from store to archive directory."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        records_path = self.output_dir / "records.jsonl"
        embeddings_path = self.output_dir / "embeddings.jsonl"

        with (
            open(records_path, "w", encoding="utf-8") as rf,
            open(embeddings_path, "w", encoding="utf-8") as ef,
        ):
            for tenant_id in tenant_ids:
                validate_tenant_id(tenant_id, allow_reserved=True)
                if not is_production_export_tenant(tenant_id):
                    logger.info("Skipping non-production tenant from portable export")
                    continue
                self._tenants.add(tenant_id)
                await self._export_blackboard(store, tenant_id, rf)
                await self._export_taxonomy(store, tenant_id, rf)
                await self._export_traces(store, tenant_id, rf)
                await self._export_episodes(store, tenant_id, rf, ef)
                await self._export_graph(store, tenant_id, rf, ef)
                await self._export_synapses(store, tenant_id, rf)

        manifest = PortableManifest(
            vector_dim=self.vector_dim,
            record_counts=self._counts,
            tenants=sorted(self._tenants),
        )
        _ = (self.output_dir / "manifest.json").write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        logger.info(
            "Archive exported: %s (%s records, %d embeddings, %d tenants)",
            self.output_dir,
            sum(self._counts.values()),
            self._emb_count,
            len(self._tenants),
        )
        return manifest

    def _write(self, handle: TextIO, record: BaseModel) -> None:
        type_name = str(getattr(record, "type", "unknown"))
        self._counts[type_name] = self._counts.get(type_name, 0) + 1
        _ = handle.write(record.model_dump_json() + "\n")

    def _write_embedding(self, handle: TextIO, ref: str, raw: bytes) -> None:
        dim = len(raw) // 4
        vec = list(struct.unpack(f"<{dim}f", raw))
        _ = handle.write(EmbeddingRecord(ref=ref, vector=vec).model_dump_json() + "\n")
        self._emb_count += 1

    async def _export_blackboard(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        handle: TextIO,
    ) -> None:
        if not is_sqlite_export_store(store):
            return
        from ..sqlite.codecs import json_dict_field

        with store.get_sqlite_connection() as db:
            cursor = db.execute(
                "SELECT * FROM blackboard WHERE tenant_id = ?",
                (tenant_id,),
            )
            bb_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in bb_rows:
                d = _row_dict(row)
                self._write(
                    handle,
                    BlackboardRecord(
                        tenant_id=tenant_id,
                        id=as_str(d.get("id")),
                        scope_path=as_str(d.get("scope_path")),
                        content=json_dict_field(d.get("content")),
                        metadata=json_dict_field(d.get("metadata")),
                        created_by=as_str(d.get("created_by")) or None,
                        created_at=as_str(d.get("created_at")),
                        ttl_until=as_str(d.get("ttl_until")) or None,
                    ),
                )

    async def _export_taxonomy(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        handle: TextIO,
    ) -> None:
        items = await store.get_all_taxonomy(tenant_id=tenant_id)
        for item in items:
            self._write(
                handle,
                TaxonomyRecord(
                    tenant_id=tenant_id,
                    domain=as_str(item.get("domain")),
                    name=as_str(item.get("name")),
                    path=as_str(item.get("path")),
                    keywords=as_str_list(item.get("keywords")),
                    metadata=as_json_dict(item.get("metadata")),
                ),
            )

    async def _export_traces(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        handle: TextIO,
    ) -> None:
        traces = await store.get_traces(tenant_id=tenant_id, limit=10000)
        for trace in traces:
            provenance_values = as_str_list(trace.get("provenance"))
            self._write(
                handle,
                TraceRecord(
                    tenant_id=tenant_id,
                    trace_id=as_str(trace.get("id")),
                    agent_id=as_str(trace.get("agent_id")),
                    session_id=as_str(trace.get("session_id")) or None,
                    user_id=as_str(trace.get("user_id")) or None,
                    graph_name=as_str(trace.get("graph_name")) or None,
                    tool_calls=as_json_dict_list(trace.get("tool_calls")),
                    token_usage=as_json_dict(trace.get("token_usage")),
                    timing_ms=as_int(trace.get("timing_ms")) or None,
                    metadata=as_json_dict(trace.get("metadata")),
                    provenance=provenance_values or None,
                    created_at=as_str(trace.get("created_at")),
                ),
            )

    async def _export_episodes(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        handle: TextIO,
        emb_handle: TextIO,
    ) -> None:
        if not is_sqlite_export_store(store):
            return
        from ..sqlite.codecs import json_dict_field

        with store.get_sqlite_connection() as db:
            cursor = db.execute(
                """
                SELECT id, user_id, session_id, content, metadata, created_at
                FROM episodic_events WHERE tenant_id = ?
                ORDER BY created_at
                """,
                (tenant_id,),
            )
            ep_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in ep_rows:
                d = _row_dict(row)
                episode_id = as_str(d.get("id"))
                emb_ref = f"emb:episode:{episode_id}"
                has_emb = False
                if store.has_sqlite_vec():
                    emb_cur = db.execute(
                        "SELECT embedding FROM vec_episodic_events WHERE event_id = ?",
                        (episode_id,),
                    )
                    emb_row = fetchone_row(emb_cur)
                    if emb_row is not None:
                        embedding_cell: object = sqlite_cell(emb_row, "embedding")
                        if isinstance(embedding_cell, (bytes, bytearray)) and embedding_cell:
                            self._write_embedding(emb_handle, emb_ref, bytes(embedding_cell))
                            has_emb = True
                self._write(
                    handle,
                    EpisodeRecord(
                        tenant_id=tenant_id,
                        user_id=as_str(d.get("user_id")),
                        episode_id=episode_id,
                        content=as_str(d.get("content")),
                        session_id=as_str(d.get("session_id")) or None,
                        metadata=json_dict_field(d.get("metadata")),
                        created_at=as_str(d.get("created_at")),
                        embedding_ref=emb_ref if has_emb else None,
                    ),
                )

    async def _export_graph(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        records_handle: TextIO,
        emb_handle: TextIO,
    ) -> None:
        if not is_sqlite_export_store(store):
            return
        from ..sqlite.codecs import json_dict_field

        with store.get_sqlite_connection() as db:
            cursor = db.execute(
                "SELECT * FROM cells WHERE tenant_id = ?",
                (tenant_id,),
            )
            cell_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in cell_rows:
                d = _row_dict(row)
                node_id = as_str(d.get("id"))
                emb_ref = f"emb:node:{node_id}"
                has_emb = False
                if store.has_sqlite_vec():
                    emb_cur = db.execute(
                        "SELECT embedding FROM vec_cells WHERE node_id = ?",
                        (node_id,),
                    )
                    emb_row = fetchone_row(emb_cur)
                    if emb_row is not None:
                        embedding_cell: object = sqlite_cell(emb_row, "embedding")
                        if isinstance(embedding_cell, (bytes, bytearray)) and embedding_cell:
                            self._write_embedding(emb_handle, emb_ref, bytes(embedding_cell))
                            has_emb = True
                self._write(
                    records_handle,
                    CellRecord(
                        tenant_id=tenant_id,
                        id=node_id,
                        content=as_str(d.get("content")),
                        cell_kind=as_str(d.get("cell_kind"), default="concept"),
                        source_type=as_str(d.get("source_type"), default="manual"),
                        source_ref=as_str(d.get("source_ref")) or None,
                        scope_path=as_str(d.get("scope_path")) or None,
                        content_hash=as_str(d.get("content_hash")),
                        confidence=as_float(d.get("confidence")),
                        visibility=as_str(d.get("visibility"), default="tenant"),
                        metadata=json_dict_field(d.get("struct_data")),
                        user_id=as_str(d.get("user_id")) or None,
                        created_at=as_str(d.get("created_at")),
                        updated_at=as_str(d.get("updated_at")),
                        embedding_ref=emb_ref if has_emb else None,
                    ),
                )

            cursor = db.execute(
                "SELECT * FROM cell_edges WHERE tenant_id = ?",
                (tenant_id,),
            )
            cell_edge_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in cell_edge_rows:
                d = _row_dict(row)
                self._write(
                    records_handle,
                    CellEdgeRecord(
                        tenant_id=tenant_id,
                        source_id=as_str(d.get("source_id")),
                        target_id=as_str(d.get("target_id")),
                        relation=as_str(d.get("relation")),
                        weight=as_float(d.get("weight"), default=1.0),
                        metadata=json_dict_field(d.get("metadata")),
                    ),
                )

    async def _export_synapses(
        self,
        store: BrainStorageProtocol,
        tenant_id: str,
        handle: TextIO,
    ) -> None:
        if not is_sqlite_export_store(store):
            return
        from ..sqlite.codecs import json_dict_field

        with store.get_sqlite_connection() as db:
            cursor = db.execute(
                "SELECT * FROM synapses WHERE tenant_id = ? ORDER BY created_at, id",
                (tenant_id,),
            )
            rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in rows:
                d = _row_dict(row)
                self._write(
                    handle,
                    SynapseRecord(
                        tenant_id=tenant_id,
                        id=as_str(d.get("id")),
                        agent_id=as_str(d.get("agent_id")),
                        action_type=as_str(d.get("action_type")),
                        action_data=json_dict_field(d.get("action_data")),
                        action_data_ref=as_str(d.get("action_data_ref")) or None,
                        thought_trace_ref=as_str(d.get("thought_trace_ref")) or None,
                        content_hash=as_str(d.get("content_hash")) or None,
                        graph_name=as_str(d.get("graph_name")) or None,
                        graph_run_id=as_str(d.get("graph_run_id")) or None,
                        node_id=as_str(d.get("node_id")) or None,
                        node_name=as_str(d.get("node_name")) or None,
                        node_role=as_str(d.get("node_role"), default="worker"),
                        scope_path=as_str(d.get("scope_path")) or None,
                        context_summary=as_str(d.get("context_summary")) or None,
                        client_id=as_str(d.get("client_id")) or None,
                        fault_class=as_str(d.get("fault_class")) or None,
                        status=as_str(d.get("status"), default="active"),
                        q_action=as_float(d.get("q_action"), default=0.5),
                        q_hypothesis=as_float(d.get("q_hypothesis"), default=0.5),
                        q_relevance=as_float(d.get("q_relevance"), default=0.5),
                        q_composite=as_float(d.get("q_composite"), default=0.5),
                        metadata=json_dict_field(d.get("metadata")),
                        created_at=as_str(d.get("created_at")),
                        updated_at=as_str(d.get("updated_at")),
                    ),
                )


__all__ = ["BrainPortableArchiveWriter"]
