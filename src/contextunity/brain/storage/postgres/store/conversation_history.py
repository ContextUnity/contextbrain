"""Canonical Conversation History operations for PostgreSQL."""

from __future__ import annotations

from abc import ABC
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
from contextunity.core.types import JsonDict
from psycopg.errors import UniqueViolation

from contextunity.brain.core.exceptions import BrainValidationError

from .base import PostgresStoreBase
from .helpers import Json, fetch_all


class ConversationHistoryMixin(PostgresStoreBase, ABC):
    """PostgreSQL implementation of the Brain-owned history contract."""

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
        """Append idempotently under the durable tenant/key constraint."""
        # The idempotency key is tenant-scoped, so duplicate resolution must see
        # the authoritative row even when a conflicting retry changes user_id.
        # Caller authorization is enforced before this owner-storage boundary;
        # the connection remains tenant-scoped and cannot cross tenants.
        async with await self.tenant_connection(tenant_id) as conn:
            try:
                inserted = await fetch_all(
                    conn,
                    """
                    INSERT INTO conversation_records
                        (record_id, tenant_id, user_id, session_id, role, kind,
                         content, content_hash, source_hash, graph_run_id,
                         metadata_version, idempotency_key, metadata, created_at)
                    VALUES
                        (%(record_id)s, %(tenant_id)s, %(user_id)s, %(session_id)s,
                         %(role)s, %(kind)s, %(content)s, %(content_hash)s,
                         %(source_hash)s, %(graph_run_id)s, %(metadata_version)s,
                         %(idempotency_key)s, %(metadata)s,
                         COALESCE(%(created_at)s, now()))
                    ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                    RETURNING record_id, content_hash, source_hash
                    """,
                    {
                        "record_id": record_id,
                        "tenant_id": tenant_id,
                        "user_id": user_id,
                        "session_id": session_id,
                        "role": role,
                        "kind": kind,
                        "content": content,
                        "content_hash": content_hash,
                        "source_hash": source_hash,
                        "graph_run_id": graph_run_id,
                        "metadata_version": metadata_version,
                        "idempotency_key": idempotency_key,
                        "metadata": Json(metadata),
                        "created_at": created_at,
                    },
                )
            except UniqueViolation as exc:
                raise BrainValidationError("conversation record identity conflicts") from exc
            if inserted:
                return ConversationAppendReceipt(
                    record_id=record_id,
                    outcome="created",
                    content_hash=content_hash,
                    source_hash=source_hash,
                )
            existing = await fetch_all(
                conn,
                """
                SELECT record_id, tenant_id, user_id, session_id, role, kind,
                       content, content_hash, source_hash, graph_run_id,
                       metadata_version, idempotency_key, metadata, created_at
                FROM conversation_records
                WHERE tenant_id = %(tenant_id)s
                  AND idempotency_key = %(idempotency_key)s
                """,
                {"tenant_id": tenant_id, "idempotency_key": idempotency_key},
            )
            if not existing:
                raise BrainValidationError("conversation duplicate receipt is unavailable")
            durable_record = ConversationRecord.model_validate(existing[0])
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
        """Return one bounded projection with SQLite-equivalent ordering."""
        clauses: dict[ConversationProjection, tuple[str, object]] = {
            "recent": ("user_id = %(selector)s", user_id),
            "session": ("session_id = %(selector)s", session_id),
            "trace_related": ("graph_run_id = %(selector)s", graph_run_id),
            "older_than": (
                "created_at < now() - make_interval(days => %(selector)s)",
                older_than_days,
            ),
        }
        clause, selector = clauses[projection]
        if selector is None:
            raise BrainValidationError("conversation projection selector is required")
        order = "DESC" if projection in {"recent", "session", "trace_related"} else "ASC"
        async with await self.tenant_connection(tenant_id, user_id=user_id) as conn:
            rows = await fetch_all(
                conn,
                f"""
                SELECT record_id, tenant_id, user_id, session_id, role, kind,
                       content, content_hash, source_hash, graph_run_id,
                       metadata_version, idempotency_key, metadata, created_at
                FROM conversation_records
                WHERE tenant_id = %(tenant_id)s AND {clause}
                ORDER BY created_at {order}, record_id {order}
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                {
                    "tenant_id": tenant_id,
                    "selector": selector,
                    "limit": limit,
                    "offset": offset,
                },
            )
        return [ConversationRecord.model_validate(row) for row in rows]

    async def get_conversation_history_stats(self, *, tenant_id: str) -> ConversationHistoryStats:
        """Return content-free tenant statistics."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT count(*) AS total, min(created_at) AS oldest,
                       max(created_at) AS newest
                FROM conversation_records WHERE tenant_id = %(tenant_id)s
                """,
                {"tenant_id": tenant_id},
            )
        row = rows[0] if rows else {"total": 0, "oldest": None, "newest": None}
        return ConversationHistoryStats.model_validate({"tenant_id": tenant_id, **row})

    async def apply_conversation_retention(
        self,
        *,
        tenant_id: str,
        record_ids: list[UUID],
        cutoff: datetime,
        policy_version: Literal["contextunity.conversation-retention/v1"],
        hold_evidence_hash: str,
    ) -> ConversationRetentionReceipt:
        """Delete explicit tenant records only when older than the cutoff."""
        async with await self.tenant_connection(tenant_id) as conn:
            cursor = await conn.execute(
                """
                DELETE FROM conversation_records
                WHERE tenant_id = %(tenant_id)s
                  AND record_id = ANY(%(record_ids)s)
                  AND created_at < %(cutoff)s
                """,
                {"tenant_id": tenant_id, "record_ids": record_ids, "cutoff": cutoff},
            )
        return ConversationRetentionReceipt(
            tenant_id=tenant_id,
            deleted_count=cursor.rowcount or 0,
            policy_version=policy_version,
            hold_evidence_hash=hold_evidence_hash,
        )


__all__ = ["ConversationHistoryMixin"]
