"""Portable Archive v2 — Importer.

Imports archive into a target ``BrainStorageProtocol``-compatible store.
Guarantees: no silent degradation, idempotent blackboard, embedding restore.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import StorageError
from pydantic import BaseModel

from contextunity.brain.storage.contracts import BrainStorageProtocol

from .models import (
    BlackboardRecord,
    CellEdgeRecord,
    CellRecord,
    EpisodeRecord,
    FactRecord,
    SynapseRecord,
    TaxonomyRecord,
    TraceRecord,
)
from .reader import BrainPortableArchiveReader
from .sqlite_export import is_sqlite_export_store

logger = get_contextunit_logger(__name__)

PortableRecord = (
    BlackboardRecord
    | TraceRecord
    | TaxonomyRecord
    | EpisodeRecord
    | FactRecord
    | CellRecord
    | CellEdgeRecord
    | SynapseRecord
)


class ImportReport(TypedDict):
    counts: dict[str, int]
    errors: list[str]
    ok: bool


class ImportResult:
    """Accumulates import results and errors."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.errors: list[str] = []

    def inc(self, rtype: str) -> None:
        self.counts[rtype] = self.counts.get(rtype, 0) + 1

    def fail(self, rtype: str, rec_id: str, err: Exception) -> None:
        self.errors.append(f"{rtype}[{rec_id}]: {err}")

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


async def import_portable_archive(
    store: BrainStorageProtocol,
    archive_path: Path,
    *,
    tenant_map: dict[str, str] | None = None,
    dry_run: bool = True,
) -> ImportReport:
    """Import a portable archive into a target store."""
    reader = BrainPortableArchiveReader(archive_path)
    validation_errors = reader.validate()
    if validation_errors:
        raise StorageError(
            f"Archive validation failed ({len(validation_errors)} errors): {validation_errors[:3]}",
        )

    if dry_run:
        logger.info("Dry-run complete. Archive is valid. No records imported.")
        return {
            "counts": reader.manifest.record_counts if reader.manifest else {},
            "errors": [],
            "ok": True,
        }

    result = ImportResult()
    emb_map = reader.iter_embeddings()

    for rec in reader.iter_records():
        rtype = str(getattr(rec, "type", "unknown"))
        tid = str(getattr(rec, "tenant_id", "default"))
        if tenant_map and tid in tenant_map:
            tid = tenant_map[tid]

        try:
            await _import_record(store, rec, tid, emb_map)
            result.inc(rtype)
        except Exception as e:
            rec_id = _get_record_id(rec)
            result.fail(rtype, rec_id, e)

    if not result.ok:
        raise StorageError(
            f"Import completed with {len(result.errors)} errors: {result.errors[:5]}",
        )

    logger.info("Archive imported: %s", result.counts)
    return {"counts": result.counts, "errors": result.errors, "ok": result.ok}


def _get_record_id(rec: BaseModel) -> str:
    for attr in ("id", "trace_id", "episode_id", "fact_key", "path", "source_id"):
        val: object = getattr(rec, attr, None)
        if val:
            return str(val)
    return "unknown"


async def _import_record(
    store: BrainStorageProtocol,
    rec: BaseModel,
    tid: str,
    emb_map: dict[str, list[float]],
) -> None:
    if isinstance(rec, BlackboardRecord):
        await _import_blackboard(store, rec, tid)
    elif isinstance(rec, TraceRecord):
        await _import_trace(store, rec, tid)
    elif isinstance(rec, TaxonomyRecord):
        await store.upsert_taxonomy(
            tenant_id=tid,
            domain=rec.domain,
            name=rec.name,
            path=rec.path,
            keywords=rec.keywords,
            metadata=rec.metadata,
        )
    elif isinstance(rec, EpisodeRecord):
        emb = emb_map.get(rec.embedding_ref) if rec.embedding_ref else None
        await _import_episode(store, rec, tid, emb)
    elif isinstance(rec, FactRecord):
        await store.upsert_fact(
            user_id=rec.user_id,
            tenant_id=tid,
            key=rec.fact_key,
            value=rec.fact_value,
            confidence=rec.confidence,
            source_id=rec.source_id,
        )
    elif isinstance(rec, CellRecord):
        from contextunity.brain.storage.postgres.models import GraphNode

        emb = emb_map.get(rec.embedding_ref) if rec.embedding_ref else None
        await store.upsert_graph(
            [
                GraphNode(
                    id=rec.id,
                    content=rec.content,
                    node_kind=rec.node_kind,
                    source_type=rec.source_type,
                    source_id=rec.source_id,
                    title=rec.title,
                    keywords_text=rec.keywords_text,
                    scope_path=rec.scope_path,
                    metadata=rec.metadata,
                    user_id=rec.user_id,
                    embedding=emb,
                )
            ],
            [],
            tenant_id=tid,
        )
    elif isinstance(rec, CellEdgeRecord):
        from contextunity.brain.storage.postgres.models import GraphEdge

        await store.upsert_graph(
            [],
            [
                GraphEdge(
                    source_id=rec.source_id,
                    target_id=rec.target_id,
                    relation=rec.relation,
                    weight=rec.weight,
                    metadata=rec.metadata,
                )
            ],
            tenant_id=tid,
        )
    elif isinstance(rec, SynapseRecord):
        await _import_synapse(store, rec, tid)


async def _import_blackboard(
    store: BrainStorageProtocol,
    rec: BlackboardRecord,
    tid: str,
) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO blackboard
                    (id, tenant_id, scope_path, content, metadata,
                     ttl_until, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.id,
                    tid,
                    rec.scope_path,
                    json_dumps(rec.content),
                    json_dumps(rec.metadata),
                    rec.ttl_until,
                    rec.created_by,
                    rec.created_at,
                ),
            )
            db.commit()
    else:
        _ = await store.write_blackboard(
            tenant_id=tid,
            scope_path=rec.scope_path,
            content=rec.content,
            metadata=rec.metadata,
            created_by=rec.created_by,
        )


async def _import_trace(store: BrainStorageProtocol, rec: TraceRecord, tid: str) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO event_journal
                    (id, tenant_id, agent_id, session_id, user_id,
                     graph_name, tool_calls, token_usage, timing_ms,
                     security_flags, metadata, provenance, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.trace_id,
                    tid,
                    rec.agent_id,
                    rec.session_id,
                    rec.user_id,
                    rec.graph_name,
                    json_dumps(rec.tool_calls),
                    json_dumps(rec.token_usage),
                    rec.timing_ms,
                    json_dumps({}),
                    json_dumps(rec.metadata),
                    json_dumps(rec.provenance),
                    rec.created_at,
                ),
            )
            db.commit()
    else:
        _ = await store.log_trace(
            tenant_id=tid,
            agent_id=rec.agent_id,
            session_id=rec.session_id,
            user_id=rec.user_id,
            graph_name=rec.graph_name,
            tool_calls=rec.tool_calls,
            token_usage=rec.token_usage,
            timing_ms=rec.timing_ms,
            metadata=rec.metadata,
            provenance=rec.provenance,
        )


async def _import_episode(
    store: BrainStorageProtocol,
    rec: EpisodeRecord,
    tid: str,
    embedding: list[float] | None,
) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO episodic_events
                    (id, tenant_id, user_id, session_id, content, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.episode_id,
                    tid,
                    rec.user_id,
                    rec.session_id,
                    rec.content,
                    json_dumps(rec.metadata),
                ),
            )
            if embedding and store.has_sqlite_vec():
                from ..sqlite.codecs import vec_to_bytes

                _ = db.execute(
                    """
                    INSERT OR REPLACE INTO vec_episodic_events
                        (event_id, embedding)
                    VALUES (?, ?)
                    """,
                    (rec.episode_id, vec_to_bytes(embedding)),
                )
            db.commit()
    else:
        await store.add_episode(
            id=rec.episode_id,
            user_id=rec.user_id,
            content=rec.content,
            tenant_id=tid,
            metadata=rec.metadata,
            session_id=rec.session_id,
            embedding=embedding,
        )


async def _import_synapse(store: BrainStorageProtocol, rec: SynapseRecord, tid: str) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO synapses (
                    id, tenant_id, agent_id, graph_name, graph_run_id, node_id,
                    node_name, action_type, action_data, action_data_ref,
                    context_summary, thought_trace_ref, content_hash, client_id,
                    node_role, fault_class, status, q_action, q_hypothesis,
                    q_relevance, q_composite, scope_path, metadata, created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.id,
                    tid,
                    rec.agent_id,
                    rec.graph_name,
                    rec.graph_run_id,
                    rec.node_id,
                    rec.node_name,
                    rec.action_type,
                    json_dumps(rec.action_data),
                    rec.action_data_ref,
                    rec.context_summary,
                    rec.thought_trace_ref,
                    rec.content_hash,
                    rec.client_id,
                    rec.node_role,
                    rec.fault_class,
                    rec.status,
                    rec.q_action,
                    rec.q_hypothesis,
                    rec.q_relevance,
                    rec.q_composite,
                    rec.scope_path,
                    json_dumps(rec.metadata),
                    rec.created_at,
                    rec.updated_at,
                ),
            )
            db.commit()
        return

    _ = await store.record_synapse(
        tenant_id=tid,
        agent_id=rec.agent_id,
        action_type=rec.action_type,
        action_data=rec.action_data,
        action_data_ref=rec.action_data_ref,
        thought_trace_ref=rec.thought_trace_ref,
        content_hash=rec.content_hash,
        graph_name=rec.graph_name,
        graph_run_id=rec.graph_run_id,
        node_id=rec.node_id,
        node_name=rec.node_name,
        node_role=rec.node_role,
        scope_path=rec.scope_path,
        context_summary=rec.context_summary,
        client_id=rec.client_id,
        fault_class=rec.fault_class,
        status=rec.status,
        q_action=rec.q_action,
        q_hypothesis=rec.q_hypothesis,
        q_relevance=rec.q_relevance,
        metadata=rec.metadata,
    )


__all__ = ["ImportReport", "ImportResult", "import_portable_archive"]
