"""Portable Archive v1 — Importer.

Imports archive into a target ``KnowledgeStoreProtocol``-compatible store.
Guarantees: no silent degradation, idempotent blackboard, embedding restore.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import StorageError
from pydantic import BaseModel

from contextunity.brain.storage.contracts import KnowledgeStoreProtocol

from .models import (
    BlackboardRecord,
    EpisodeRecord,
    FactRecord,
    KnowledgeEdgeRecord,
    KnowledgeNodeRecord,
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
    | KnowledgeNodeRecord
    | KnowledgeEdgeRecord
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
    store: KnowledgeStoreProtocol,
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
    store: KnowledgeStoreProtocol,
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
    elif isinstance(rec, KnowledgeNodeRecord):
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
                    taxonomy_path=rec.taxonomy_path,
                    metadata=rec.metadata,
                    user_id=rec.user_id,
                    embedding=emb,
                )
            ],
            [],
            tenant_id=tid,
        )
    elif isinstance(rec, KnowledgeEdgeRecord):
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


async def _import_blackboard(
    store: KnowledgeStoreProtocol,
    rec: BlackboardRecord,
    tid: str,
) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO blackboard_records
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


async def _import_trace(store: KnowledgeStoreProtocol, rec: TraceRecord, tid: str) -> None:
    from ..sqlite.codecs import json_dumps

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            _ = db.execute(
                """
                INSERT OR REPLACE INTO agent_traces
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
    store: KnowledgeStoreProtocol,
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


__all__ = ["ImportReport", "ImportResult", "import_portable_archive"]
