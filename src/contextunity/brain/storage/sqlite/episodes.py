"""Episodic memory storage (SQLite implementation).

Contract-compatible with ``postgres/store/episodes.py``.
"""

from __future__ import annotations

import sqlite3

from contextunity.core.narrowing import as_int, as_str
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.exceptions import BrainValidationError

from .codecs import fetchone_row, json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin


class EpisodesMixin(SqliteConnectionMixin):
    """SQLite episode operations matching Postgres contract."""

    async def add_episode(
        self,
        *,
        id: str,
        user_id: str,
        content: str,
        tenant_id: str,
        embedding: list[float] | None = None,
        metadata: JsonDict | None = None,
        session_id: str | None = None,
    ) -> None:
        """Add an episodic event."""
        if not tenant_id:
            raise BrainValidationError("tenant_id is required for add_episode")

        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO episodic_events
                    (id, tenant_id, user_id, session_id, content, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (id, tenant_id, user_id, session_id, content, json_dumps(metadata or {})),
            )

            # Store embedding in vector table if available
            if embedding and self.has_sqlite_vec():
                from .codecs import vec_to_bytes

                _ = db.execute(
                    """
                    INSERT INTO vec_episodic_events (event_id, embedding)
                    VALUES (?, ?)
                    ON CONFLICT (event_id) DO UPDATE SET
                        embedding = excluded.embedding
                    """,
                    (id, vec_to_bytes(embedding)),
                )
            db.commit()

    async def get_recent_episodes(
        self, *, user_id: str, tenant_id: str, limit: int = 5
    ) -> list[JsonDict]:
        """Get recent episodes for a user."""
        with self._get_connection() as db:
            cursor = db.execute(
                """
                SELECT id, content, metadata, created_at
                FROM episodic_events
                WHERE tenant_id = ? AND user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (tenant_id, user_id, limit),
            )
            rows: list[sqlite3.Row] = list(cursor.fetchall())

        episodes: list[JsonDict] = []
        for r in rows:
            meta_cell = sqlite_cell(r, "metadata")
            meta_raw = json_loads(meta_cell if isinstance(meta_cell, str) else None)
            episodes.append(
                {
                    "id": as_str(sqlite_cell(r, "id")),
                    "content": as_str(sqlite_cell(r, "content")),
                    "metadata": meta_raw if is_json_dict(meta_raw) else {},
                    "created_at": as_str(sqlite_cell(r, "created_at")),
                }
            )
        return episodes

    async def count_episodes(self, *, tenant_id: str) -> JsonDict:
        """Get episode count and date range for a tenant.

        Returns dict with total, oldest, newest — matching Postgres shape.
        """
        with self._get_connection() as db:
            cursor = db.execute(
                """
                SELECT
                    count(*) as total,
                    min(created_at) as oldest,
                    max(created_at) as newest
                FROM episodic_events
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            )
            count_row = fetchone_row(cursor)

        if count_row is not None:
            oldest_cell = sqlite_cell(count_row, "oldest")
            newest_cell = sqlite_cell(count_row, "newest")
            return {
                "total": as_int(sqlite_cell(count_row, "total")),
                "oldest": oldest_cell if isinstance(oldest_cell, str) else None,
                "newest": newest_cell if isinstance(newest_cell, str) else None,
            }
        return {"total": 0, "oldest": None, "newest": None}

    async def get_old_episodes(
        self,
        *,
        tenant_id: str = "default",
        older_than_days: int = 30,
        limit: int = 100,
    ) -> list[JsonDict]:
        """Get episodes older than N days for distillation."""
        # SQLite date arithmetic: subtract days
        with self._get_connection() as db:
            cursor = db.execute(
                """
                SELECT id, user_id, content, metadata, created_at
                FROM episodic_events
                WHERE tenant_id = ?
                  AND created_at < datetime('now', ? || ' days')
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (tenant_id, f"-{older_than_days}", limit),
            )
            rows: list[sqlite3.Row] = list(cursor.fetchall())

        episodes: list[JsonDict] = []
        for r in rows:
            meta_cell = sqlite_cell(r, "metadata")
            meta_raw = json_loads(meta_cell if isinstance(meta_cell, str) else None)
            episodes.append(
                {
                    "id": as_str(sqlite_cell(r, "id")),
                    "user_id": as_str(sqlite_cell(r, "user_id")),
                    "content": as_str(sqlite_cell(r, "content")),
                    "metadata": meta_raw if is_json_dict(meta_raw) else {},
                    "created_at": as_str(sqlite_cell(r, "created_at")),
                }
            )
        return episodes

    async def delete_old_episodes(
        self,
        *,
        tenant_id: str = "default",
        older_than_days: int = 30,
        episode_ids: list[str] | None = None,
    ) -> int:
        """Delete episodes older than N days (or by explicit IDs)."""
        with self._get_connection() as db:
            if episode_ids:
                placeholders = ", ".join("?" for _ in episode_ids)
                # Also clean vector table
                if self.has_sqlite_vec():
                    _ = db.execute(
                        f"DELETE FROM vec_episodic_events WHERE event_id IN ({placeholders})",
                        episode_ids,
                    )
                cursor = db.execute(
                    f"DELETE FROM episodic_events WHERE tenant_id = ? AND id IN ({placeholders})",
                    [tenant_id, *episode_ids],
                )
            else:
                # Clean vector table first
                if self.has_sqlite_vec():
                    _ = db.execute(
                        """
                        DELETE FROM vec_episodic_events WHERE event_id IN (
                            SELECT id FROM episodic_events
                            WHERE tenant_id = ?
                              AND created_at < datetime('now', ? || ' days')
                        )
                        """,
                        (tenant_id, f"-{older_than_days}"),
                    )
                cursor = db.execute(
                    """
                    DELETE FROM episodic_events
                    WHERE tenant_id = ?
                      AND created_at < datetime('now', ? || ' days')
                    """,
                    (tenant_id, f"-{older_than_days}"),
                )
            db.commit()
            return cursor.rowcount or 0


__all__ = ["EpisodesMixin"]
