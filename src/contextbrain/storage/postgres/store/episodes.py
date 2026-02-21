"""Episodic memory operations."""

from __future__ import annotations

from typing import Any, List

from .helpers import Json, execute, fetch_all, vec


class EpisodesMixin:
    """Mixin for episodic memory operations."""

    async def add_episode(
        self,
        *,
        id: str,
        user_id: str,
        content: str,
        tenant_id: str,
        embedding: List[float] | None = None,
        metadata: dict = None,
        session_id: str | None = None,
    ) -> None:
        """Add an episodic event."""
        async with await self.tenant_connection(tenant_id) as conn:
            await execute(
                conn,
                """
                INSERT INTO episodic_events (id, tenant_id, user_id, session_id, content, embedding, metadata)
                VALUES (%(id)s, %(tenant_id)s, %(user_id)s, %(session_id)s, %(content)s, %(embedding)s, %(metadata)s)
            """,
                {
                    "id": id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "session_id": session_id,
                    "content": content,
                    "embedding": vec(embedding) if embedding else None,
                    "metadata": Json(metadata or {}),
                },
            )

    async def get_recent_episodes(
        self, *, user_id: str, tenant_id: str, limit: int = 5
    ) -> List[dict]:
        """Get recent episodes for a user."""
        async with await self.tenant_connection(tenant_id) as conn:
            return await fetch_all(
                conn,
                """
                SELECT id, content, metadata, created_at FROM episodic_events
                WHERE tenant_id = %(tenant_id)s AND user_id = %(user_id)s
                ORDER BY created_at DESC LIMIT %(limit)s
            """,
                {"tenant_id": tenant_id, "user_id": user_id, "limit": limit},
            )

    async def upsert_fact(
        self,
        *,
        user_id: str,
        tenant_id: str = "default",
        key: str,
        value: Any,
        confidence: float = 1.0,
        source_id: str | None = None,
    ) -> None:
        """Upsert a user fact."""
        async with await self.tenant_connection(tenant_id) as conn:
            await execute(
                conn,
                """
                INSERT INTO user_facts (tenant_id, user_id, fact_key, fact_value, confidence, source_id, updated_at)
                VALUES (%(tenant_id)s, %(user_id)s, %(key)s, %(value)s, %(confidence)s, %(source_id)s, now())
                ON CONFLICT (tenant_id, user_id, fact_key) DO UPDATE SET
                    fact_value = EXCLUDED.fact_value, confidence = EXCLUDED.confidence,
                    source_id = EXCLUDED.source_id, updated_at = now()
            """,
                {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "key": key,
                    "value": value,
                    "confidence": confidence,
                    "source_id": source_id,
                },
            )

    async def get_user_facts(
        self,
        *,
        user_id: str,
        tenant_id: str = "default",
    ) -> List[dict]:
        """Get all facts for a user.

        Returns list of dicts with: fact_key, fact_value, confidence, updated_at.
        """
        async with await self.tenant_connection(tenant_id) as conn:
            return await fetch_all(
                conn,
                """
                SELECT fact_key, fact_value, confidence, updated_at
                FROM user_facts
                WHERE tenant_id = %(tenant_id)s AND user_id = %(user_id)s
                ORDER BY updated_at DESC
            """,
                {"tenant_id": tenant_id, "user_id": user_id},
            )

    # ── Retention & Distillation ──

    async def get_old_episodes(
        self,
        *,
        tenant_id: str = "default",
        older_than_days: int = 30,
        limit: int = 100,
    ) -> List[dict]:
        """Get episodes older than N days for distillation.

        Returns list of dicts with: id, user_id, content, metadata, created_at.
        """
        async with await self.tenant_connection(tenant_id) as conn:
            return await fetch_all(
                conn,
                """
                SELECT id, user_id, content, metadata, created_at
                FROM episodic_events
                WHERE tenant_id = %(tenant_id)s
                  AND created_at < now() - make_interval(days => %(days)s)
                ORDER BY created_at ASC
                LIMIT %(limit)s
            """,
                {"tenant_id": tenant_id, "days": older_than_days, "limit": limit},
            )

    async def delete_old_episodes(
        self,
        *,
        tenant_id: str = "default",
        older_than_days: int = 30,
        episode_ids: List[str] | None = None,
    ) -> int:
        """Delete episodes older than N days (or by explicit IDs).

        Returns count of deleted rows.
        """
        async with await self.tenant_connection(tenant_id) as conn:
            if episode_ids:
                # Delete specific episodes (after distillation)
                cur = await conn.execute(
                    """
                    DELETE FROM episodic_events
                    WHERE tenant_id = %(tenant_id)s AND id = ANY(%(ids)s)
                """,
                    {"tenant_id": tenant_id, "ids": episode_ids},
                )
            else:
                # Bulk delete by age
                cur = await conn.execute(
                    """
                    DELETE FROM episodic_events
                    WHERE tenant_id = %(tenant_id)s
                      AND created_at < now() - make_interval(days => %(days)s)
                """,
                    {"tenant_id": tenant_id, "days": older_than_days},
                )
            return cur.rowcount or 0

    async def count_episodes(
        self,
        *,
        tenant_id: str = "default",
    ) -> dict:
        """Get episode count and date range for a tenant."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT
                    count(*) as total,
                    min(created_at) as oldest,
                    max(created_at) as newest
                FROM episodic_events
                WHERE tenant_id = %(tenant_id)s
            """,
                {"tenant_id": tenant_id},
            )
            if rows:
                return rows[0]
            return {"total": 0, "oldest": None, "newest": None}
