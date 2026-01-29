"""Postgres knowledge store implementation (pgvector + ltree)."""

from __future__ import annotations

import logging
from typing import Any, Iterable, List, Optional

from psycopg import errors as psycopg_errors
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from contextbrain.core.exceptions import StorageError

from .models import GraphEdge, GraphNode, KnowledgeStoreInterface, SearchResult, TaxonomyPath

logger = logging.getLogger(__name__)


def _format_vector(vec: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


class PostgresKnowledgeStore(KnowledgeStoreInterface):
    def __init__(
        self,
        *,
        dsn: str,
        pool_min_size: int = 5,
        pool_max_size: int = 20,
    ) -> None:
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
                timeout=60.0,  # Wait up to 60s for a connection
                open=False,
            )
        # Check if pool is not yet open
        if not self._pool._opened:
            await self._pool.open()
        return self._pool

    async def upsert_graph(
        self,
        nodes: List[GraphNode],
        edges: List[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.transaction():
                conn.row_factory = dict_row
                for node in nodes:
                    await conn.execute(
                        """
                        INSERT INTO knowledge_nodes (
                            id, tenant_id, user_id, node_kind, source_type, source_id, title,
                            content, struct_data, keywords_text, content_hash, taxonomy_path, embedding
                        )
                        VALUES (
                            %(id)s, %(tenant_id)s, %(user_id)s, %(node_kind)s, %(source_type)s, %(source_id)s,
                            %(title)s, %(content)s, %(struct_data)s, %(keywords_text)s, %(content_hash)s, %(taxonomy_path)s,
                            %(embedding)s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            content = EXCLUDED.content,
                            struct_data = EXCLUDED.struct_data,
                            keywords_text = EXCLUDED.keywords_text,
                            taxonomy_path = EXCLUDED.taxonomy_path,
                            embedding = EXCLUDED.embedding
                        """,
                        {
                            "id": node.id,
                            "tenant_id": tenant_id,
                            "user_id": user_id,
                            "node_kind": node.node_kind,
                            "source_type": node.source_type,
                            "source_id": node.source_id,
                            "title": node.title,
                            "content": node.content,
                            "struct_data": Json(node.metadata),
                            "keywords_text": node.keywords_text,
                            "content_hash": None,
                            "taxonomy_path": node.taxonomy_path,
                            "embedding": _format_vector(node.embedding) if node.embedding else None,
                        },
                    )
                for edge in edges:
                    await conn.execute(
                        """
                        INSERT INTO knowledge_edges (
                            tenant_id, source_id, target_id, relation, weight, metadata
                        )
                        VALUES (%(tenant_id)s, %(source_id)s, %(target_id)s, %(relation)s, %(weight)s, %(metadata)s)
                        ON CONFLICT (tenant_id, source_id, target_id, relation) DO UPDATE SET
                            weight = EXCLUDED.weight,
                            metadata = EXCLUDED.metadata
                        """,
                        {
                            "tenant_id": tenant_id,
                            "source_id": edge.source_id,
                            "target_id": edge.target_id,
                            "relation": edge.relation,
                            "weight": edge.weight,
                            "metadata": edge.metadata,
                        },
                    )

    async def add_episode(
        self,
        *,
        id: str,
        user_id: str,
        content: str,
        embedding: List[float] | None = None,
        metadata: dict,
        tenant_id: str,
        session_id: str | None = None,
    ) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
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
                    "embedding": _format_vector(embedding) if embedding else None,
                    "metadata": Json(metadata),
                },
            )

    async def get_recent_episodes(
        self, *, user_id: str, tenant_id: str, limit: int = 5
    ) -> List[dict]:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            rows = await conn.execute(
                """
                SELECT id, content, metadata, created_at FROM episodic_events
                WHERE tenant_id = %(tenant_id)s AND user_id = %(user_id)s
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                {"tenant_id": tenant_id, "user_id": user_id, "limit": limit},
            )
            return await (await rows).fetchall()

    async def upsert_fact(
        self,
        *,
        user_id: str,
        key: str,
        value: Any,
        confidence: float = 1.0,
        source_id: str | None = None,
    ) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO user_facts (user_id, fact_key, fact_value, confidence, source_id, updated_at)
                VALUES (%(user_id)s, %(key)s, %(value)s, %(confidence)s, %(source_id)s, now())
                ON CONFLICT (user_id, fact_key) DO UPDATE SET
                    fact_value = EXCLUDED.fact_value,
                    confidence = EXCLUDED.confidence,
                    source_id = EXCLUDED.source_id,
                    updated_at = now()
                """,
                {
                    "user_id": user_id,
                    "key": key,
                    "value": value,
                    "confidence": confidence,
                    "source_id": source_id,
                },
            )

    async def upsert_taxonomy(
        self,
        *,
        tenant_id: str,
        domain: str,
        name: str,
        path: str,
        keywords: List[str],
        metadata: dict = None,
    ) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO catalog_taxonomy (tenant_id, domain, name, path, keywords, metadata, updated_at)
                VALUES (%(tenant_id)s, %(domain)s, %(name)s, %(path)s, %(keywords)s, %(metadata)s, now())
                ON CONFLICT (tenant_id, domain, path) DO UPDATE SET
                    name = EXCLUDED.name,
                    keywords = EXCLUDED.keywords,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
                """,
                {
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "name": name,
                    "path": path,
                    "keywords": keywords,
                    "metadata": Json(metadata or {}),
                },
            )

    async def get_all_taxonomy(self, *, tenant_id: str, domain: Optional[str] = None) -> List[dict]:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            query = "SELECT * FROM catalog_taxonomy WHERE tenant_id = %(tenant_id)s"
            params = {"tenant_id": tenant_id}
            if domain:
                query += " AND domain = %(domain)s"
                params["domain"] = domain

            rows = await conn.execute(query, params)
            return await (await rows).fetchall()

    async def hybrid_search(
        self,
        *,
        query_text: str,
        query_vec: List[float],
        candidate_k: int = 50,
        limit: int = 8,
        scope: TaxonomyPath | None = None,
        source_types: List[str] | None = None,
        graph_depth: int = 2,
        allowed_relations: List[str] | None = None,
        fusion: str = "weighted",
        rrf_k: int = 60,
        vector_weight: float = 0.8,
        text_weight: float = 0.2,
        tenant_id: str,
        user_id: str | None = None,
    ) -> List[SearchResult]:
        _ = graph_depth, allowed_relations
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if candidate_k <= 0 or limit <= 0:
            return []
        candidate_k = max(1, candidate_k)
        limit = min(limit, candidate_k)

        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            vector_hits = await self._fetch_vector_hits(
                conn=conn,
                tenant_id=tenant_id,
                user_id=user_id,
                query_vec=query_vec,
                candidate_k=candidate_k,
                scope=scope,
                source_types=source_types,
            )
            text_hits = await self._fetch_text_hits(
                conn=conn,
                tenant_id=tenant_id,
                user_id=user_id,
                query_text=query_text,
                candidate_k=candidate_k,
                scope=scope,
                source_types=source_types,
            )
            ranked_ids = self._fuse_results(
                vector_hits=vector_hits,
                text_hits=text_hits,
                fusion=fusion,
                rrf_k=rrf_k,
                vector_weight=vector_weight,
                text_weight=text_weight,
                limit=limit,
            )
            if not ranked_ids:
                return []
            nodes = await self._fetch_nodes(conn=conn, tenant_id=tenant_id, ids=ranked_ids)
            node_map = {n.id: n for n in nodes}
            return [
                SearchResult(
                    node=node_map[rid],
                    score=score,
                    vector_score=vector_hits.get(rid),
                    text_score=text_hits.get(rid),
                )
                for rid, score in ranked_ids
                if rid in node_map
            ]

    async def _fetch_vector_hits(
        self,
        *,
        conn,
        tenant_id: str,
        user_id: str | None,
        query_vec: List[float],
        candidate_k: int,
        scope: TaxonomyPath | None,
        source_types: List[str] | None,
    ) -> dict[str, float]:
        clauses, params = self._build_scope_filters(
            tenant_id=tenant_id,
            user_id=user_id,
            scope=scope,
            source_types=source_types,
        )
        where_sql = sql.SQL(" AND ").join(clauses)
        query = (
            sql.SQL(
                """
            SELECT id, 1 - (embedding <=> %s::vector) AS vector_score
            FROM knowledge_nodes
            WHERE node_kind = 'chunk'
              AND embedding IS NOT NULL
              AND
            """
            )
            + where_sql
            + sql.SQL(
                """
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """
            )
        )
        return await self._fetch_scores(
            conn=conn,
            sql=query,
            params=[_format_vector(query_vec), *params, _format_vector(query_vec), candidate_k],
            score_key="vector_score",
        )

    async def _fetch_text_hits(
        self,
        *,
        conn,
        tenant_id: str,
        user_id: str | None,
        query_text: str,
        candidate_k: int,
        scope: TaxonomyPath | None,
        source_types: List[str] | None,
    ) -> dict[str, float]:
        if not query_text.strip():
            return {}
        clauses, params = self._build_scope_filters(
            tenant_id=tenant_id,
            user_id=user_id,
            scope=scope,
            source_types=source_types,
        )
        where_sql = sql.SQL(" AND ").join(clauses)
        query = (
            sql.SQL(
                """
            SELECT id,
                   ts_rank_cd(
                       search_vector || COALESCE(keywords_vector, ''::tsvector),
                       websearch_to_tsquery('simple', %s)
                   ) AS text_score
            FROM knowledge_nodes
            WHERE node_kind = 'chunk'
              AND (
                  search_vector || COALESCE(keywords_vector, ''::tsvector)
              ) @@ websearch_to_tsquery('simple', %s)
              AND
            """
            )
            + where_sql
            + sql.SQL(
                """
            ORDER BY text_score DESC
            LIMIT %s
            """
            )
        )
        return await self._fetch_scores(
            conn=conn,
            sql=query,
            params=[query_text, query_text, *params, candidate_k],
            score_key="text_score",
        )

    async def _fetch_scores(
        self, *, conn, sql: sql.Composed, params: list, score_key: str
    ) -> dict[str, float]:
        try:
            rows = await conn.execute(sql, params)
        except psycopg_errors.UndefinedColumn as e:
            # Schema mismatch - likely missing column/index
            column_info = str(e).split("column ")[-1].split()[0].strip('"') if "column " in str(e) else "unknown"
            logger.error(f"Database schema error: missing column '{column_info}'. Run migrations.")
            raise StorageError(
                f"Missing database column: {column_info}. Please run migrations.",
                code="SCHEMA_MISMATCH",
            ) from e
        except psycopg_errors.DatabaseError as e:
            logger.error(f"Database error during search: {e}")
            raise StorageError(f"Database query failed: {e}", code="DB_QUERY_ERROR") from e

        out: dict[str, float] = {}
        async for row in rows:
            rid = str(row["id"])
            score = row.get(score_key)
            if score is None:
                continue
            out[rid] = float(score)
        return out

    async def _fetch_nodes(self, *, conn, tenant_id: str, ids: Iterable[str]) -> List[GraphNode]:
        rows = await conn.execute(
            """
            SELECT id, node_kind, source_type, source_id, title, content, struct_data, taxonomy_path, tenant_id, user_id
            FROM knowledge_nodes
            WHERE tenant_id = %s AND id = ANY(%s::text[])
            """,
            [tenant_id, list(ids)],
        )
        nodes: list[GraphNode] = []
        async for row in rows:
            nodes.append(
                GraphNode(
                    id=row["id"],
                    node_kind=row["node_kind"],
                    source_type=row.get("source_type"),
                    source_id=row.get("source_id"),
                    title=row.get("title"),
                    content=row.get("content") or "",
                    metadata=row.get("struct_data") or {},
                    taxonomy_path=row.get("taxonomy_path"),
                    tenant_id=row.get("tenant_id"),
                    user_id=row.get("user_id"),
                )
            )
        return nodes

    def _fuse_results(
        self,
        *,
        vector_hits: dict[str, float],
        text_hits: dict[str, float],
        fusion: str,
        rrf_k: int,
        vector_weight: float,
        text_weight: float,
        limit: int,
    ) -> list[tuple[str, float]]:
        ids = set(vector_hits) | set(text_hits)
        if not ids:
            return []
        if fusion == "rrf":
            vec_rank = {rid: rank for rank, rid in enumerate(vector_hits.keys(), start=1)}
            text_rank = {rid: rank for rank, rid in enumerate(text_hits.keys(), start=1)}
            scored = []
            for rid in ids:
                score = 0.0
                if rid in vec_rank:
                    score += 1.0 / (rrf_k + vec_rank[rid])
                if rid in text_rank:
                    score += 1.0 / (rrf_k + text_rank[rid])
                scored.append((rid, score))
        else:
            scored = [
                (
                    rid,
                    (vector_weight * vector_hits.get(rid, 0.0))
                    + (text_weight * text_hits.get(rid, 0.0)),
                )
                for rid in ids
            ]
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:limit]

    def _build_scope_filters(
        self,
        *,
        tenant_id: str,
        user_id: str | None,
        scope: TaxonomyPath | None,
        source_types: List[str] | None,
    ) -> tuple[list[sql.Composed], list]:
        where: list[sql.Composed] = [sql.SQL("tenant_id = %s")]
        params: list = [tenant_id]
        if user_id:
            where.append(sql.SQL("(user_id = %s OR user_id IS NULL)"))
            params.append(user_id)
        if scope:
            where.append(sql.SQL("taxonomy_path <@ %s::ltree"))
            params.append(scope.path)
        if source_types:
            where.append(sql.SQL("source_type = ANY(%s::text[])"))
            params.append(source_types)
        return where, params

    # ==========================================================================
    # NewsEngine Methods (Pink Pony)
    # ==========================================================================

    async def upsert_news_raw(
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
        """Upsert a raw news item from harvest."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO news_raw (id, tenant_id, url, headline, summary, category, source_api, metadata, harvested_at)
                VALUES (%(id)s, %(tenant_id)s, %(url)s, %(headline)s, %(summary)s, %(category)s, %(source_api)s, %(metadata)s, now())
                ON CONFLICT (tenant_id, url) DO UPDATE SET
                    headline = EXCLUDED.headline,
                    summary = EXCLUDED.summary,
                    category = EXCLUDED.category,
                    metadata = EXCLUDED.metadata,
                    harvested_at = now()
                RETURNING id
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

    async def upsert_news_fact(
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
        """Upsert a validated fact from archivist."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO news_facts (
                    id, tenant_id, url, headline, summary, category, suggested_agent,
                    significance, atomic_facts, irony_potential, embedding, metadata, raw_id, created_at
                )
                VALUES (
                    %(id)s, %(tenant_id)s, %(url)s, %(headline)s, %(summary)s, %(category)s, %(suggested_agent)s,
                    %(significance)s, %(atomic_facts)s, %(irony_potential)s, %(embedding)s, %(metadata)s, %(raw_id)s, now()
                )
                ON CONFLICT (tenant_id, url) DO UPDATE SET
                    headline = EXCLUDED.headline,
                    summary = EXCLUDED.summary,
                    category = EXCLUDED.category,
                    suggested_agent = EXCLUDED.suggested_agent,
                    significance = EXCLUDED.significance,
                    atomic_facts = EXCLUDED.atomic_facts,
                    irony_potential = EXCLUDED.irony_potential,
                    embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata
                RETURNING id
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
                    "embedding": _format_vector(embedding) if embedding else None,
                    "metadata": Json(metadata or {}),
                    "raw_id": raw_id,
                },
            )
        return id

    async def get_news_facts(
        self,
        *,
        tenant_id: str,
        limit: int = 20,
        since: str | None = None,
    ) -> List[dict]:
        """Get recent facts for planning."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            query = """
                SELECT id, url, headline, summary, category, suggested_agent,
                       significance, atomic_facts, irony_potential, metadata, created_at
                FROM news_facts
                WHERE tenant_id = %(tenant_id)s
            """
            params: dict = {"tenant_id": tenant_id, "limit": limit}
            if since:
                query += " AND created_at >= %(since)s"
                params["since"] = since
            query += " ORDER BY created_at DESC LIMIT %(limit)s"
            result = await conn.execute(query, params)
            return await result.fetchall()

    async def search_news_facts(
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
                WHERE tenant_id = %(tenant_id)s
                  AND embedding IS NOT NULL
                  AND 1 - (embedding <=> %(vec)s::vector) >= %(min_score)s
                ORDER BY embedding <=> %(vec)s::vector
                LIMIT %(limit)s
                """,
                {
                    "tenant_id": tenant_id,
                    "vec": _format_vector(query_vec),
                    "min_score": min_score,
                    "limit": limit,
                },
            )
            return await result.fetchall()

    async def upsert_news_post(
        self,
        *,
        id: str,
        tenant_id: str,
        fact_id: str | None = None,
        agent: str,
        headline: str,
        content: str,
        emoji: str = "📰",
        fact_url: str | None = None,
        embedding: List[float] | None = None,
        scheduled_at: str | None = None,
    ) -> str:
        """Upsert a generated post."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO news_posts (
                    id, tenant_id, fact_id, agent, headline, content, emoji,
                    fact_url, embedding, scheduled_at, created_at
                )
                VALUES (
                    %(id)s, %(tenant_id)s, %(fact_id)s, %(agent)s, %(headline)s, %(content)s, %(emoji)s,
                    %(fact_url)s, %(embedding)s, %(scheduled_at)s, now()
                )
                ON CONFLICT (id) DO UPDATE SET
                    content = EXCLUDED.content,
                    scheduled_at = EXCLUDED.scheduled_at
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
                    "embedding": _format_vector(embedding) if embedding else None,
                    "scheduled_at": scheduled_at,
                },
            )
        return id

    async def mark_post_published(
        self,
        *,
        post_id: str,
        telegram_msg_id: int,
    ) -> None:
        """Mark a post as published."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(
                """
                UPDATE news_posts SET published_at = now(), telegram_msg_id = %(msg_id)s
                WHERE id = %(id)s
                """,
                {"id": post_id, "msg_id": telegram_msg_id},
            )

    async def search_news_posts(
        self,
        *,
        tenant_id: str,
        query_vec: List[float],
        limit: int = 3,
    ) -> List[dict]:
        """Vector search for similar past posts (RAG context)."""
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                """
                SELECT id, agent, headline, content, emoji,
                       1 - (embedding <=> %(vec)s::vector) AS score
                FROM news_posts
                WHERE tenant_id = %(tenant_id)s
                  AND embedding IS NOT NULL
                  AND published_at IS NOT NULL
                ORDER BY embedding <=> %(vec)s::vector
                LIMIT %(limit)s
                """,
                {
                    "tenant_id": tenant_id,
                    "vec": _format_vector(query_vec),
                    "limit": limit,
                },
            )
            return await result.fetchall()


__all__ = ["PostgresKnowledgeStore"]
