"""Blackboard storage — Flat Memory (SQLite implementation).

Contract-compatible with ``postgres/store/blackboard.py``:
- ``write_blackboard`` returns ``{id, scope_path, created_at}``
- ``read_blackboard`` accepts ``ids`` list, returns record dicts
- ``prune_expired_blackboard`` deletes records past ``ttl_until``
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str
from contextunity.core.types import JsonDict, is_json_dict

from .codecs import json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin

logger = get_contextunit_logger(__name__)


class BlackboardMixin(SqliteConnectionMixin):
    """SQLite blackboard operations matching Postgres contract."""

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
        """Write a blackboard record, return its UUID."""
        record_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        ttl_until = None
        if ttl_seconds and ttl_seconds > 0:
            ttl_until = (now + timedelta(seconds=ttl_seconds)).isoformat()

        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO blackboard
                    (id, tenant_id, scope_path, content, metadata,
                     ttl_until, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    tenant_id,
                    scope_path,
                    json_dumps(content),
                    json_dumps(metadata or {}),
                    ttl_until,
                    created_by,
                    now.isoformat(),
                ),
            )
            db.commit()

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
        """Read blackboard records by UUIDs, excluding expired TTL."""
        if not ids:
            return []

        placeholders = ", ".join("?" for _ in ids)
        now = datetime.now(timezone.utc).isoformat()

        # Single query: fetch every matching id (expired or not) and filter
        # in Python, so we can log an expired-ref count separately from
        # "never existed" without a second round trip.
        with self._get_connection() as db:
            cursor = db.execute(
                f"""
                SELECT id, content, metadata, scope_path, created_at, created_by,
                       (ttl_until IS NOT NULL AND ttl_until <= ?) AS is_expired
                FROM blackboard
                WHERE id IN ({placeholders})
                  AND tenant_id = ?
                """,
                (now, *ids, tenant_id),
            )
            all_rows: list[sqlite3.Row] = list(cursor.fetchall())

        rows = [row for row in all_rows if not sqlite_cell(row, "is_expired")]
        expired_count = len(all_rows) - len(rows)
        rows.sort(key=lambda row: as_str(sqlite_cell(row, "created_at")))

        records: list[JsonDict] = []
        for row in rows:
            content_cell = sqlite_cell(row, "content")
            meta_cell = sqlite_cell(row, "metadata")
            content_raw = json_loads(content_cell if isinstance(content_cell, str) else None)
            meta_raw = json_loads(meta_cell if isinstance(meta_cell, str) else None)
            created_by_cell = sqlite_cell(row, "created_by")
            created_by: str | None = created_by_cell if isinstance(created_by_cell, str) else None
            record: JsonDict = {
                "id": as_str(sqlite_cell(row, "id")),
                "content": content_raw if is_json_dict(content_raw) else {},
                "metadata": meta_raw if is_json_dict(meta_raw) else {},
                "scope_path": as_str(sqlite_cell(row, "scope_path")),
                "created_at": as_str(sqlite_cell(row, "created_at")),
                "created_by": created_by,
            }
            records.append(record)

        logger.debug(
            "Blackboard read: requested=%d found=%d expired=%d tenant=%s",
            len(ids),
            len(records),
            expired_count,
            tenant_id,
        )
        return records

    async def prune_expired_blackboard(self, *, tenant_id: str | None = None) -> int:
        """Delete blackboard records whose TTL has expired."""
        now = datetime.now(timezone.utc).isoformat()

        with self._get_connection() as db:
            if tenant_id:
                cursor = db.execute(
                    "DELETE FROM blackboard WHERE ttl_until < ? AND tenant_id = ?",
                    (now, tenant_id),
                )
            else:
                cursor = db.execute(
                    "DELETE FROM blackboard WHERE ttl_until < ? AND ttl_until IS NOT NULL",
                    (now,),
                )
            db.commit()
            deleted = cursor.rowcount or 0

        if deleted > 0:
            logger.info("Blackboard prune: deleted=%d tenant=%s", deleted, tenant_id or "all")
        return deleted


__all__ = ["BlackboardMixin"]
