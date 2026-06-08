"""Blackboard storage operations — Flat Memory Phase A.

Provides async methods for writing and reading ephemeral scratch data
used by graph execution for pass-by-reference communication.
"""

from __future__ import annotations

import uuid
from abc import ABC
from datetime import datetime, timezone

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str
from contextunity.core.parsing import json_dumps
from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, is_json_dict
from psycopg import sql

from .base import PostgresStoreBase

logger = get_contextunit_logger(__name__)


class BlackboardStoreMixin(PostgresStoreBase, ABC):
    """Mixin that adds blackboard CRUD operations to the PostgresStore.

    Requires the host class to provide `tenant_connection(tenant_id)`.
    """

    async def write_blackboard(
        self,
        *,
        tenant_id: str,
        scope_path: str,
        content: JsonDict,
        metadata: JsonDict | None = None,
        ttl_seconds: int | None = None,
        created_by: str | None = None,
    ) -> JsonDict:
        """Write a blackboard record, return its UUID.

        Args:
            tenant_id: Tenant isolation key.
            scope_path: LTREE path (e.g. 'tenant.project.session.step').
            content: JSONB content to store.
            metadata: Optional metadata dict.
            ttl_seconds: Optional TTL in seconds. NULL means no expiry.
            created_by: Optional agent_id or node_name.

        Returns:
            Dict with {id, scope_path, created_at}.
        """
        record_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        ttl_until = None
        if ttl_seconds and ttl_seconds > 0:
            from datetime import timedelta

            ttl_until = now + timedelta(seconds=ttl_seconds)

        async with await self.tenant_connection(tenant_id) as conn:
            _ = await conn.execute(
                """
                INSERT INTO blackboard_records
                    (id, tenant_id, scope_path, content, metadata, ttl_until, created_by, created_at)
                VALUES
                    (%s, %s, %s::ltree, %s::jsonb, %s::jsonb, %s, %s, %s)
                """,
                (
                    record_id,
                    tenant_id,
                    scope_path,
                    json_dumps(content),
                    json_dumps(metadata or {}),
                    ttl_until,
                    created_by,
                    now,
                ),
            )

        logger.debug(
            "Blackboard write: id=%s scope=%s tenant=%s ttl=%s",
            record_id,
            scope_path,
            tenant_id,
            ttl_seconds,
        )

        return {
            "id": record_id,
            "scope_path": scope_path,
            "created_at": now.isoformat(),
        }

    async def read_blackboard(
        self,
        *,
        ids: list[str],
        tenant_id: str,
    ) -> list[JsonDict]:
        """Read blackboard records by UUIDs — strictly batched.

        Excludes records whose TTL has expired.

        Args:
            ids: List of UUID strings to read.
            tenant_id: Tenant isolation key (RLS + WHERE filter).

        Returns:
            List of record dicts: [{id, content, metadata, scope_path, created_at}].
        """
        if not ids:
            return []

        # Build parameterized IN clause
        placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(ids))

        async with await self.tenant_connection(tenant_id) as conn:
            cursor = await conn.execute(
                sql.SQL(
                    (
                        "SELECT id, content, metadata, scope_path::text, created_at, created_by"
                        " FROM blackboard_records"
                        " WHERE id IN ({}) AND tenant_id = %s"
                        " AND (ttl_until IS NULL OR ttl_until > now())"
                        " ORDER BY created_at"
                    )
                ).format(placeholders),
                (*ids, tenant_id),
            )
            rows: list[tuple[object, ...]] = await cursor.fetchall()

        records: list[JsonDict] = []
        for row in rows:
            content_cell: object = row[1]
            if is_json_dict(content_cell):
                content_val = content_cell
            elif isinstance(content_cell, str):
                loaded = parse_wire_json(content_cell)
                content_val = loaded if is_json_dict(loaded) else {}
            else:
                content_val = {}
            meta_cell: object = row[2]
            if is_json_dict(meta_cell):
                meta_val = meta_cell
            elif isinstance(meta_cell, str):
                loaded_meta = parse_wire_json(meta_cell)
                meta_val = loaded_meta if is_json_dict(loaded_meta) else {}
            else:
                meta_val = {}
            created_by_cell: object = row[5]
            records.append(
                {
                    "id": as_str(row[0]),
                    "content": content_val,
                    "metadata": meta_val,
                    "scope_path": as_str(row[3]),
                    "created_at": as_str(row[4]),
                    "created_by": (
                        created_by_cell
                        if isinstance(created_by_cell, (str, type(None)))
                        else as_str(created_by_cell)
                    ),
                }
            )

        logger.debug(
            "Blackboard read: requested=%d found=%d tenant=%s",
            len(ids),
            len(records),
            tenant_id,
        )

        return records

    async def prune_expired_blackboard(self, *, tenant_id: str | None = None) -> int:
        """Delete blackboard records whose TTL has expired.

        This is the application-layer counterpart to the pg_cron job.
        Defence-in-depth: pg_cron handles autonomous cleanup;
        this method provides metrics/logging for Worker synthesis workflow.

        Args:
            tenant_id: Optional tenant filter. None = prune all tenants.

        Returns:
            Number of deleted records.
        """
        effective_tenant = tenant_id or "*"
        async with await self.tenant_connection(effective_tenant) as conn:
            if tenant_id:
                cursor = await conn.execute(
                    """
                    DELETE FROM blackboard_records
                    WHERE ttl_until < now() AND tenant_id = %s
                    """,
                    (tenant_id,),
                )
            else:
                cursor = await conn.execute(
                    "DELETE FROM blackboard_records WHERE ttl_until < now()"
                )
            deleted = cursor.rowcount or 0

        if deleted > 0:
            logger.info(
                "Blackboard prune: deleted=%d tenant=%s",
                deleted,
                tenant_id or "all",
            )

        return deleted


__all__ = ["BlackboardStoreMixin"]
