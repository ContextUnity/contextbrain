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
        pool = await self._get_pool()
        async with pool.connection() as conn:
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
        pool = await self._get_pool()
        async with pool.connection() as conn:
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
        key: str,
        value: Any,
        confidence: float = 1.0,
        source_id: str | None = None,
    ) -> None:
        """Upsert a user fact."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await execute(
                conn,
                """
                INSERT INTO user_facts (user_id, fact_key, fact_value, confidence, source_id, updated_at)
                VALUES (%(user_id)s, %(key)s, %(value)s, %(confidence)s, %(source_id)s, now())
                ON CONFLICT (user_id, fact_key) DO UPDATE SET
                    fact_value = EXCLUDED.fact_value, confidence = EXCLUDED.confidence,
                    source_id = EXCLUDED.source_id, updated_at = now()
            """,
                {
                    "user_id": user_id,
                    "key": key,
                    "value": value,
                    "confidence": confidence,
                    "source_id": source_id,
                },
            )
