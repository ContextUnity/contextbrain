"""Portable Archive importer.

Imports archive into a target ``BrainStorageProtocol``-compatible store.
Guarantees: no silent degradation, idempotent blackboard, embedding restore.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol, TypedDict
from uuid import UUID

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import StorageError
from contextunity.core.narrowing import (
    as_str,
    as_str_list,
    json_dict_list_as_json,
    str_list_as_json,
)
from contextunity.core.types import JsonDict
from pydantic import BaseModel

from contextunity.brain.storage.contracts import BrainStorageProtocol

from .models import (
    BlackboardRecord,
    CellEdgeRecord,
    CellRecord,
    ConversationArchiveRecord,
    OutcomeObservationArchiveRecord,
    SynapseRecord,
    TraceRecord,
)
from .reader import BrainPortableArchiveReader
from .sqlite_export import is_sqlite_export_store

logger = get_contextunit_logger(__name__)


class TraceImportStore(Protocol):
    """Exact storage boundary needed by portable Trace import."""

    async def log_trace(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        session_id: str | None = None,
        user_id: str | None = None,
        graph_name: str | None = None,
        tool_calls: list[JsonDict] | None = None,
        token_usage: JsonDict | None = None,
        timing_ms: int | None = None,
        security_flags: JsonDict | None = None,
        metadata: JsonDict | None = None,
        provenance: list[str] | None = None,
    ) -> str: ...

    async def finalize_execution_trace(self, *, terminal_trace: JsonDict) -> JsonDict: ...


PortableRecord = (
    BlackboardRecord
    | TraceRecord
    | ConversationArchiveRecord
    | OutcomeObservationArchiveRecord
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
    for attr in ("id", "trace_id", "record_id", "path", "source_id"):
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
    elif isinstance(rec, ConversationArchiveRecord):
        await _import_conversation_record(store, rec, tid)
    elif isinstance(rec, OutcomeObservationArchiveRecord):
        dumped = rec.model_dump(mode="json", exclude={"type", "tenant_id"})
        await store.import_outcome_observation_record(tenant_id=tid, record=dumped)
    elif isinstance(rec, CellRecord):
        emb = emb_map.get(rec.embedding_ref) if rec.embedding_ref else None
        await store.upsert_cell(
            tenant_id=tid,
            cell_id=rec.id,
            user_id=rec.user_id,
            cell_kind=rec.cell_kind,
            content=rec.content,
            metadata=rec.metadata,
            scope_path=rec.scope_path,
            content_hash=rec.content_hash,
            source_type=rec.source_type,
            source_ref=rec.source_ref,
            confidence=rec.confidence,
            visibility=rec.visibility,
        )
        if emb is not None:
            await store.restore_cell_embedding(
                tenant_id=tid,
                cell_id=rec.id,
                vector=emb,
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


def _terminal_trace_from_record(
    rec: TraceRecord,
    tid: str,
    control_evidence: JsonDict,
) -> JsonDict:
    terminal_trace: JsonDict = {
        "schema_version": rec.trace_schema_version,
        "trace_id": rec.trace_id,
        "graph_run_id": rec.graph_run_id,
        "tenant_id": tid,
        "agent_id": rec.agent_id,
        "session_id": rec.session_id,
        "user_id": rec.user_id,
        "project_id": as_str(rec.metadata.get("project_id")),
        "graph_name": rec.graph_name or "",
        "terminal_status": rec.terminal_status,
        "terminal_reason": rec.terminal_reason,
        "duration_ms": rec.timing_ms or 0,
        "steps": json_dict_list_as_json(rec.steps),
        "usage": rec.token_usage,
        "prompt_evidence": json_dict_list_as_json(rec.prompt_evidence),
        "control_evidence": control_evidence,
        "provenance": str_list_as_json(rec.provenance or []),
        "security_flags": str_list_as_json(as_str_list(rec.security_flags.get("codes"))),
        "digest": rec.payload_digest,
    }
    if rec.final_verdict:
        terminal_trace["final_verdict"] = rec.final_verdict
    registration_hash = as_str(rec.metadata.get("registration_hash"))
    if registration_hash:
        terminal_trace["registration_hash"] = registration_hash
    for field_name in ("plan_id", "parent_plan_id", "replan_ref"):
        value = as_str(rec.metadata.get(field_name))
        if value:
            terminal_trace[field_name] = value
    for field_name in ("plan_revision", "parent_plan_revision"):
        value = rec.metadata.get(field_name)
        if isinstance(value, int) and not isinstance(value, bool):
            terminal_trace[field_name] = value
    return terminal_trace


def _validate_v5_terminal_record(terminal_trace: JsonDict) -> None:
    from contextunity.brain.payloads.memory import TerminalExecutionTracePayload

    _ = TerminalExecutionTracePayload.model_validate(terminal_trace)


async def _import_trace(store: TraceImportStore, rec: TraceRecord, tid: str) -> None:
    from ..sqlite.codecs import json_dumps

    control_evidence = rec.control_evidence
    if rec.trace_schema_version in {
        "contextunity.execution-trace/v2",
        "contextunity.execution-trace/v3",
        "contextunity.execution-trace/v4",
        "contextunity.execution-trace/v5",
    }:
        from contextunity.brain.payloads.memory import TraceControlEvidencePayload

        control_evidence = TraceControlEvidencePayload.model_validate(control_evidence).model_dump(
            mode="json", exclude_unset=True
        )

    terminal_trace = _terminal_trace_from_record(rec, tid, control_evidence)
    if rec.trace_schema_version in {
        "contextunity.execution-trace/v5",
        "contextunity.execution-trace/v6",
    }:
        _validate_v5_terminal_record(terminal_trace)

    if is_sqlite_export_store(store):
        with store.get_sqlite_connection() as db:
            existing = db.execute(
                """
                SELECT id, payload_digest FROM execution_traces
                WHERE id = ? OR (? IS NOT NULL AND tenant_id = ? AND graph_run_id = ?)
                LIMIT 1
                """,
                (rec.trace_id, rec.graph_run_id, tid, rec.graph_run_id),
            ).fetchone()
            if existing is not None:
                existing_digest = existing[1]
                if rec.payload_digest is None or existing_digest == rec.payload_digest:
                    return
                raise StorageError("portable trace conflicts with existing trace identity")
            _ = db.execute(
                """
                INSERT INTO execution_traces
                    (id, tenant_id, agent_id, session_id, user_id,
                     graph_name, tool_calls, token_usage, timing_ms,
                     security_flags, metadata, provenance, created_at,
                     graph_run_id, payload_digest, terminal_status, terminal_reason,
                     trace_schema_version, prompt_evidence, steps, control_evidence, final_verdict)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    json_dumps(rec.security_flags),
                    json_dumps(rec.metadata),
                    json_dumps(rec.provenance),
                    rec.created_at,
                    rec.graph_run_id,
                    rec.payload_digest,
                    rec.terminal_status,
                    rec.terminal_reason,
                    rec.trace_schema_version,
                    json_dumps(rec.prompt_evidence),
                    json_dumps(rec.steps),
                    json_dumps(control_evidence),
                    json_dumps(rec.final_verdict),
                ),
            )
            db.commit()
    else:
        if (
            rec.trace_schema_version
            in {
                "contextunity.execution-trace/v1",
                "contextunity.execution-trace/v2",
                "contextunity.execution-trace/v3",
                "contextunity.execution-trace/v4",
                "contextunity.execution-trace/v5",
            }
            and rec.graph_run_id
            and rec.payload_digest
            and rec.terminal_status
            and rec.terminal_reason
        ):
            _ = await store.finalize_execution_trace(terminal_trace=terminal_trace)
            return
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


async def _import_conversation_record(
    store: BrainStorageProtocol,
    rec: ConversationArchiveRecord,
    tid: str,
) -> None:
    _ = await store.append_conversation_record(
        record_id=UUID(rec.record_id),
        tenant_id=tid,
        user_id=rec.user_id,
        session_id=rec.session_id,
        role=rec.role,
        kind=rec.kind,
        content=rec.content,
        content_hash=rec.content_hash,
        source_hash=rec.source_hash,
        graph_run_id=UUID(rec.graph_run_id) if rec.graph_run_id is not None else None,
        metadata_version=rec.metadata_version,
        idempotency_key=rec.idempotency_key,
        metadata=rec.metadata,
        created_at=datetime.fromisoformat(rec.created_at),
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
