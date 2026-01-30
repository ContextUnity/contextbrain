"""
NewsEngine storage operations.

Separate module for news pipeline (news pipeline) - NOT part of generic store!
"""

from __future__ import annotations

from typing import List

from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool


def _vec(v: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in v) + "]"


class NewsStore:
    """Storage for news pipeline operations (news pipeline)."""

    def __init__(self, *, dsn: str, pool_min_size: int = 2, pool_max_size: int = 10):
        self._dsn = dsn
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool: AsyncConnectionPool | None = None

    async def _get_pool(self) -> AsyncConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = AsyncConnectionPool(
                self._dsn,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                timeout=60.0,
                open=False,
            )
        if not self._pool._opened:
            await self._pool.open()
        return self._pool

    async def close(self) -> None:
        if self._pool and not self._pool.closed:
            await self._pool.close()

    async def upsert_raw(
        self,
        *,
        id: str,
        tenant_id: str,
        url: str,
        headline: str,
        summary: str,
        category: str | None = None,
        source_api: str,
        metadata: dict | None = None,
    ) -> str:
        """Upsert raw news from harvest."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO news_raw (id, tenant_id, url, headline, summary, category, source_api, metadata, harvested_at)
                VALUES (%(id)s, %(tenant_id)s, %(url)s, %(headline)s, %(summary)s, %(category)s, %(source_api)s, %(metadata)s, now())
                ON CONFLICT (tenant_id, url) DO UPDATE SET
                    headline = EXCLUDED.headline, summary = EXCLUDED.summary,
                    category = EXCLUDED.category, metadata = EXCLUDED.metadata, harvested_at = now()
            """,
                {
                    "id": id,
                    "tenant_id": tenant_id,
                    "url": url,
                    "headline": headline,
                    "summary": summary,
                    "category": category,
                    "source_api": source_api,
                    "metadata": Json(metadata or {}),
                },
            )
        return id

    async def upsert_fact(
        self,
        *,
        id: str,
        tenant_id: str,
        url: str,
        headline: str,
        summary: str,
        category: str | None = None,
        suggested_agent: str | None = None,
        significance: float = 0.5,
        atomic_facts: List[str] | None = None,
        irony_potential: str | None = None,
        embedding: List[float] | None = None,
        metadata: dict | None = None,
        raw_id: str | None = None,
    ) -> str:
        """Upsert validated fact from archivist."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO news_facts (
                    id, tenant_id, url, headline, summary, category, suggested_agent,
                    significance, atomic_facts, irony_potential, embedding, metadata, raw_id, created_at
                ) VALUES (
                    %(id)s, %(tenant_id)s, %(url)s, %(headline)s, %(summary)s, %(category)s,
                    %(suggested_agent)s, %(significance)s, %(atomic_facts)s, %(irony_potential)s,
                    %(embedding)s, %(metadata)s, %(raw_id)s, now()
                )
                ON CONFLICT (tenant_id, url) DO UPDATE SET
                    headline = EXCLUDED.headline, summary = EXCLUDED.summary,
                    category = EXCLUDED.category, suggested_agent = EXCLUDED.suggested_agent,
                    significance = EXCLUDED.significance, atomic_facts = EXCLUDED.atomic_facts,
                    irony_potential = EXCLUDED.irony_potential, embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata
            """,
                {
                    "id": id,
                    "tenant_id": tenant_id,
                    "url": url,
                    "headline": headline,
                    "summary": summary,
                    "category": category,
                    "suggested_agent": suggested_agent,
                    "significance": significance,
                    "atomic_facts": atomic_facts or [],
                    "irony_potential": irony_potential,
                    "embedding": _vec(embedding) if embedding else None,
                    "metadata": Json(metadata or {}),
                    "raw_id": raw_id,
                },
            )
        return id

    async def get_facts(
        self, *, tenant_id: str, limit: int = 20, since: str | None = None
    ) -> List[dict]:
        """Get recent facts for planning."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            query = """
                SELECT id, url, headline, summary, category, suggested_agent,
                       significance, atomic_facts, irony_potential, metadata, created_at
                FROM news_facts WHERE tenant_id = %(tenant_id)s
            """
            params: dict = {"tenant_id": tenant_id, "limit": limit}
            if since:
                query += " AND created_at >= %(since)s"
                params["since"] = since
            query += " ORDER BY created_at DESC LIMIT %(limit)s"
            result = await conn.execute(query, params)
            return await result.fetchall()

    async def search_facts(
        self,
        *,
        tenant_id: str,
        query_vec: List[float],
        limit: int = 5,
        min_score: float = 0.7,
    ) -> List[dict]:
        """Vector search for similar facts (deduplication)."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                """
                SELECT id, url, headline, summary, category,
                       1 - (embedding <=> %(vec)s::vector) AS score
                FROM news_facts
                WHERE tenant_id = %(tenant_id)s AND embedding IS NOT NULL
                  AND 1 - (embedding <=> %(vec)s::vector) >= %(min_score)s
                ORDER BY embedding <=> %(vec)s::vector LIMIT %(limit)s
            """,
                {
                    "tenant_id": tenant_id,
                    "vec": _vec(query_vec),
                    "min_score": min_score,
                    "limit": limit,
                },
            )
            return await result.fetchall()

    async def upsert_post(
        self,
        *,
        id: str,
        tenant_id: str,
        agent: str,
        headline: str,
        content: str,
        fact_id: str | None = None,
        emoji: str = "📰",
        fact_url: str | None = None,
        embedding: List[float] | None = None,
        scheduled_at: str | None = None,
    ) -> str:
        """Upsert generated post. Deduplicates by (tenant_id, fact_url)."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            # Use fact_url for deduplication if available, otherwise id
            if fact_url:
                result = await conn.execute(
                    """
                    INSERT INTO news_posts (
                        id, tenant_id, fact_id, agent, headline, content, emoji,
                        fact_url, embedding, scheduled_at, created_at
                    ) VALUES (
                        %(id)s, %(tenant_id)s, %(fact_id)s, %(agent)s, %(headline)s, %(content)s,
                        %(emoji)s, %(fact_url)s, %(embedding)s, %(scheduled_at)s, now()
                    )
                    ON CONFLICT (tenant_id, fact_url)
                    DO UPDATE SET
                        content = EXCLUDED.content,
                        scheduled_at = EXCLUDED.scheduled_at,
                        embedding = EXCLUDED.embedding
                    RETURNING id
                """,
                    {
                        "id": id,
                        "tenant_id": tenant_id,
                        "fact_id": fact_id,
                        "agent": agent,
                        "headline": headline,
                        "content": content,
                        "emoji": emoji,
                        "fact_url": fact_url,
                        "embedding": _vec(embedding) if embedding else None,
                        "scheduled_at": scheduled_at if scheduled_at else None,
                    },
                )
                row = await result.fetchone()
                return row[0] if row else id
            else:
                # No fact_url - use id for conflict
                await conn.execute(
                    """
                    INSERT INTO news_posts (
                        id, tenant_id, fact_id, agent, headline, content, emoji,
                        fact_url, embedding, scheduled_at, created_at
                    ) VALUES (
                        %(id)s, %(tenant_id)s, %(fact_id)s, %(agent)s, %(headline)s, %(content)s,
                        %(emoji)s, %(fact_url)s, %(embedding)s, %(scheduled_at)s, now()
                    )
                    ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content
                """,
                    {
                        "id": id,
                        "tenant_id": tenant_id,
                        "fact_id": fact_id,
                        "agent": agent,
                        "headline": headline,
                        "content": content,
                        "emoji": emoji,
                        "fact_url": fact_url,
                        "embedding": _vec(embedding) if embedding else None,
                        "scheduled_at": scheduled_at if scheduled_at else None,
                    },
                )
                return id

    async def mark_published(self, *, post_id: str, telegram_msg_id: int) -> None:
        """Mark post as published."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                UPDATE news_posts SET published_at = now(), telegram_msg_id = %(msg_id)s
                WHERE id = %(id)s
            """,
                {"id": post_id, "msg_id": telegram_msg_id},
            )

    async def search_posts(
        self,
        *,
        tenant_id: str,
        query_vec: List[float],
        limit: int = 3,
    ) -> List[dict]:
        """Vector search for similar posts (RAG context)."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                """
                SELECT id, agent, headline, content, emoji,
                       1 - (embedding <=> %(vec)s::vector) AS score
                FROM news_posts
                WHERE tenant_id = %(tenant_id)s AND embedding IS NOT NULL AND published_at IS NOT NULL
                ORDER BY embedding <=> %(vec)s::vector LIMIT %(limit)s
            """,
                {"tenant_id": tenant_id, "vec": _vec(query_vec), "limit": limit},
            )
            return await result.fetchall()


__all__ = ["NewsStore"]
