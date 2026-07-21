"""Canonical Conversation History storage for SQLite."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Literal
from uuid import UUID

from contextunity.core.sdk.conversation import (
    ConversationAppendReceipt,
    ConversationHistoryStats,
    ConversationKind,
    ConversationProjection,
    ConversationRecord,
    ConversationRetentionReceipt,
    ConversationRole,
    conversation_record_matches_append,
)
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.exceptions import BrainValidationError

from .codecs import fetchone_row, json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin


class ConversationHistoryMixin(SqliteConnectionMixin):
    """SQLite implementation of the Brain-owned Conversation History contract."""

    async def append_conversation_record(
        self,
        *,
        record_id: UUID,
        tenant_id: str,
        user_id: str,
        session_id: str | None,
        role: ConversationRole,
        kind: ConversationKind,
        content: str,
        content_hash: str,
        source_hash: str,
        graph_run_id: UUID | None,
        metadata_version: int,
        idempotency_key: str,
        metadata: JsonDict,
        created_at: datetime | None = None,
    ) -> ConversationAppendReceipt:
        """Append idempotently; reject conflicting reuse without overwrite."""
        with self._get_connection() as db:
            existing = db.execute(
                """
                SELECT record_id, tenant_id, user_id, session_id, role, kind,
                       content, content_hash, source_hash, graph_run_id,
                       metadata_version, idempotency_key, metadata, created_at
                FROM conversation_records
                WHERE tenant_id = ? AND idempotency_key = ?
                """,
                (tenant_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                durable_record = _record_from_row(existing)
                if not conversation_record_matches_append(
                    durable_record,
                    record_id=record_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    role=role,
                    kind=kind,
                    content=content,
                    content_hash=content_hash,
                    source_hash=source_hash,
                    graph_run_id=graph_run_id,
                    metadata_version=metadata_version,
                    idempotency_key=idempotency_key,
                    metadata=metadata,
                    created_at=created_at,
                ):
                    raise BrainValidationError(
                        "conversation idempotency key conflicts with the durable record"
                    )
                return ConversationAppendReceipt(
                    record_id=record_id,
                    outcome="duplicate",
                    content_hash=content_hash,
                    source_hash=source_hash,
                )
            try:
                db.execute(
                    """
                    INSERT INTO conversation_records
                        (record_id, tenant_id, user_id, session_id, role, kind,
                         content, content_hash, source_hash, graph_run_id,
                         metadata_version, idempotency_key, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            COALESCE(?, CURRENT_TIMESTAMP))
                    """,
                    (
                        str(record_id),
                        tenant_id,
                        user_id,
                        session_id,
                        role,
                        kind,
                        content,
                        content_hash,
                        source_hash,
                        str(graph_run_id) if graph_run_id is not None else None,
                        metadata_version,
                        idempotency_key,
                        json_dumps(metadata),
                        created_at.isoformat() if created_at is not None else None,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise BrainValidationError("conversation record identity conflicts") from exc
            db.commit()
        return ConversationAppendReceipt(
            record_id=record_id,
            outcome="created",
            content_hash=content_hash,
            source_hash=source_hash,
        )

    async def query_conversation_history(
        self,
        *,
        tenant_id: str,
        projection: ConversationProjection,
        user_id: str | None,
        session_id: str | None,
        graph_run_id: UUID | None,
        older_than_days: int | None,
        limit: int,
        offset: int,
    ) -> list[ConversationRecord]:
        """Return one bounded canonical projection in deterministic order."""
        selectors: dict[ConversationProjection, tuple[str, object]] = {
            "recent": ("user_id = ?", user_id),
            "session": ("session_id = ?", session_id),
            "trace_related": (
                "graph_run_id = ?",
                str(graph_run_id) if graph_run_id is not None else None,
            ),
            "older_than": (
                "created_at < datetime('now', ? || ' days')",
                f"-{older_than_days}" if older_than_days is not None else None,
            ),
        }
        clause, selector = selectors[projection]
        if selector is None:
            raise BrainValidationError("conversation projection selector is required")
        order = "DESC" if projection in {"recent", "session", "trace_related"} else "ASC"
        with self._get_connection() as db:
            rows: list[sqlite3.Row] = list(
                db.execute(
                    f"""
                    SELECT record_id, tenant_id, user_id, session_id, role, kind,
                           content, content_hash, source_hash, graph_run_id,
                           metadata_version, idempotency_key, metadata, created_at
                    FROM conversation_records
                    WHERE tenant_id = ? AND {clause}
                    ORDER BY created_at {order}, record_id {order}
                    LIMIT ? OFFSET ?
                    """,
                    (tenant_id, selector, limit, offset),
                ).fetchall()
            )
        return [_record_from_row(row) for row in rows]

    async def get_conversation_history_stats(self, *, tenant_id: str) -> ConversationHistoryStats:
        """Return content-free tenant count and time bounds."""
        with self._get_connection() as db:
            row = fetchone_row(
                db.execute(
                    """
                    SELECT count(*) AS total, min(created_at) AS oldest,
                           max(created_at) AS newest
                    FROM conversation_records WHERE tenant_id = ?
                    """,
                    (tenant_id,),
                )
            )
        if row is None:
            return ConversationHistoryStats(tenant_id=tenant_id, total=0)
        return ConversationHistoryStats.model_validate(
            {
                "tenant_id": tenant_id,
                "total": sqlite_cell(row, "total"),
                "oldest": sqlite_cell(row, "oldest"),
                "newest": sqlite_cell(row, "newest"),
            }
        )

    async def apply_conversation_retention(
        self,
        *,
        tenant_id: str,
        record_ids: list[UUID],
        cutoff: datetime,
        policy_version: Literal["contextunity.conversation-retention/v1"],
        hold_evidence_hash: str,
    ) -> ConversationRetentionReceipt:
        """Delete only explicit tenant rows older than the verified cutoff."""
        placeholders = ", ".join("?" for _ in record_ids)
        with self._get_connection() as db:
            cursor = db.execute(
                f"""
                DELETE FROM conversation_records
                WHERE tenant_id = ? AND record_id IN ({placeholders})
                  AND created_at < ?
                """,
                [tenant_id, *(str(record_id) for record_id in record_ids), cutoff.isoformat()],
            )
            db.commit()
        return ConversationRetentionReceipt(
            tenant_id=tenant_id,
            deleted_count=cursor.rowcount or 0,
            policy_version=policy_version,
            hold_evidence_hash=hold_evidence_hash,
        )


def _record_from_row(row: sqlite3.Row) -> ConversationRecord:
    metadata_raw = json_loads(
        value if isinstance(value := sqlite_cell(row, "metadata"), str) else None
    )
    return ConversationRecord.model_validate(
        {
            "record_id": sqlite_cell(row, "record_id"),
            "tenant_id": sqlite_cell(row, "tenant_id"),
            "user_id": sqlite_cell(row, "user_id"),
            "session_id": sqlite_cell(row, "session_id"),
            "role": sqlite_cell(row, "role"),
            "kind": sqlite_cell(row, "kind"),
            "content": sqlite_cell(row, "content"),
            "content_hash": sqlite_cell(row, "content_hash"),
            "source_hash": sqlite_cell(row, "source_hash"),
            "graph_run_id": sqlite_cell(row, "graph_run_id"),
            "metadata_version": sqlite_cell(row, "metadata_version"),
            "idempotency_key": sqlite_cell(row, "idempotency_key"),
            "metadata": metadata_raw if is_json_dict(metadata_raw) else {},
            "created_at": sqlite_cell(row, "created_at"),
        }
    )


__all__ = ["ConversationHistoryMixin"]
