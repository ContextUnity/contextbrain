"""User facts storage (SQLite implementation).

Contract-compatible with ``postgres/store/episodes.py`` (EpisodesMixin.upsert_fact/get_user_facts).
"""

from __future__ import annotations

import sqlite3

from contextunity.core.types import JsonDict, JsonValue

from contextunity.brain.core.exceptions import BrainValidationError

from .codecs import row_to_dict
from .connection import SqliteConnectionMixin


class FactsMixin(SqliteConnectionMixin):
    """SQLite user facts operations matching Postgres contract."""

    async def upsert_fact(
        self,
        *,
        user_id: str,
        tenant_id: str = "default",
        key: str,
        value: JsonValue,
        confidence: float = 1.0,
        source_id: str | None = None,
    ) -> None:
        """Upsert a user fact."""
        if not tenant_id:
            raise BrainValidationError("tenant_id is required for upsert_fact")

        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO user_facts
                    (tenant_id, user_id, fact_key, fact_value, confidence,
                     source_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT (tenant_id, user_id, fact_key) DO UPDATE SET
                    fact_value = excluded.fact_value,
                    confidence = excluded.confidence,
                    source_id = excluded.source_id,
                    updated_at = datetime('now')
                """,
                (tenant_id, user_id, key, value, confidence, source_id),
            )
            db.commit()

    async def get_user_facts(self, *, user_id: str, tenant_id: str = "default") -> list[JsonDict]:
        """Get all facts for a user.

        Returns list of dicts with: fact_key, fact_value, confidence, updated_at.
        """
        with self._get_connection() as db:
            cursor = db.execute(
                """
                SELECT fact_key, fact_value, confidence, updated_at
                FROM user_facts
                WHERE tenant_id = ? AND user_id = ?
                ORDER BY updated_at DESC
                """,
                (tenant_id, user_id),
            )
            rows = cursor.fetchall()

        typed_rows: list[sqlite3.Row] = list(rows)
        return [row_to_dict(r) for r in typed_rows]


__all__ = ["FactsMixin"]
