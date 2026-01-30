"""Hybrid search operations."""

from __future__ import annotations

import logging
from typing import Iterable, List

from psycopg import errors as pg_errors
from psycopg import sql
from psycopg.rows import dict_row

from contextbrain.core.exceptions import StorageError

from ..models import GraphNode, SearchResult, TaxonomyPath
from .helpers import vec

logger = logging.getLogger(__name__)


class SearchMixin:
    """Mixin for hybrid vector + text search."""

    async def hybrid_search(
        self,
        *,
        query_text: str,
        query_vec: List[float],
        tenant_id: str,
        candidate_k: int = 50,
        limit: int = 8,
        scope: TaxonomyPath | None = None,
        source_types: List[str] | None = None,
        user_id: str | None = None,
        fusion: str = "weighted",
        rrf_k: int = 60,
        vector_weight: float = 0.8,
        text_weight: float = 0.2,
        **_,
    ) -> List[SearchResult]:
        """Hybrid vector + text search with configurable fusion."""
        if not tenant_id or candidate_k <= 0 or limit <= 0:
            return []

        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row

            where, params = self._build_scope_filters(
                tenant_id=tenant_id, user_id=user_id, scope=scope, source_types=source_types
            )
            where_sql = sql.SQL(" AND ").join(where)

            # Vector search
            vec_query = (
                sql.SQL("""
                SELECT id, 1 - (embedding <=> %s::vector) AS score FROM knowledge_nodes
                WHERE node_kind = 'chunk' AND embedding IS NOT NULL AND
            """)
                + where_sql
                + sql.SQL(" ORDER BY embedding <=> %s::vector LIMIT %s")
            )

            vector_hits = await self._fetch_scores(
                conn, vec_query, [vec(query_vec), *params, vec(query_vec), candidate_k], "score"
            )

            # Text search
            text_hits = {}
            if query_text.strip():
                text_query = (
                    sql.SQL("""
                    SELECT id, ts_rank_cd(
                        search_vector || COALESCE(keywords_vector, ''::tsvector),
                        websearch_to_tsquery('simple', %s)
                    ) AS score FROM knowledge_nodes
                    WHERE node_kind = 'chunk' AND (
                        search_vector || COALESCE(keywords_vector, ''::tsvector)
                    ) @@ websearch_to_tsquery('simple', %s) AND
                """)
                    + where_sql
                    + sql.SQL(" ORDER BY score DESC LIMIT %s")
                )

                text_hits = await self._fetch_scores(
                    conn, text_query, [query_text, query_text, *params, candidate_k], "score"
                )

            # Fuse results
            ranked = self._fuse_results(
                vector_hits, text_hits, fusion, rrf_k, vector_weight, text_weight, limit
            )

            if not ranked:
                return []

            nodes = await self._fetch_nodes(conn, tenant_id, [r[0] for r in ranked])
            node_map = {n.id: n for n in nodes}

            return [
                SearchResult(
                    node=node_map[rid],
                    score=score,
                    vector_score=vector_hits.get(rid),
                    text_score=text_hits.get(rid),
                )
                for rid, score in ranked
                if rid in node_map
            ]

    def _build_scope_filters(
        self,
        *,
        tenant_id: str,
        user_id: str | None,
        scope: TaxonomyPath | None,
        source_types: List[str] | None,
    ) -> tuple[list, list]:
        """Build WHERE clause filters."""
        where = [sql.SQL("tenant_id = %s")]
        params = [tenant_id]
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

    async def _fetch_scores(self, conn, query, params: list, key: str) -> dict[str, float]:
        """Execute query and extract scores."""
        try:
            rows = await conn.execute(query, params)
        except pg_errors.UndefinedColumn as e:
            raise StorageError(f"Schema mismatch: {e}", code="SCHEMA_MISMATCH") from e
        except pg_errors.DatabaseError as e:
            raise StorageError(f"Query failed: {e}", code="DB_QUERY_ERROR") from e

        return {str(r["id"]): float(r[key]) async for r in rows if r.get(key) is not None}

    async def _fetch_nodes(self, conn, tenant_id: str, ids: Iterable[str]) -> List[GraphNode]:
        """Fetch full node data."""
        rows = await conn.execute(
            """
            SELECT id, node_kind, source_type, source_id, title, content,
                   struct_data, taxonomy_path, tenant_id, user_id
            FROM knowledge_nodes WHERE tenant_id = %s AND id = ANY(%s::text[])
        """,
            [tenant_id, list(ids)],
        )

        return [
            GraphNode(
                id=r["id"],
                node_kind=r["node_kind"],
                content=r.get("content") or "",
                source_type=r.get("source_type"),
                source_id=r.get("source_id"),
                title=r.get("title"),
                metadata=r.get("struct_data") or {},
                taxonomy_path=r.get("taxonomy_path"),
                tenant_id=r.get("tenant_id"),
                user_id=r.get("user_id"),
            )
            async for r in rows
        ]

    def _fuse_results(
        self,
        vec_hits: dict,
        txt_hits: dict,
        fusion: str,
        rrf_k: int,
        vec_w: float,
        txt_w: float,
        limit: int,
    ) -> list[tuple[str, float]]:
        """Fuse vector and text search results."""
        ids = set(vec_hits) | set(txt_hits)
        if not ids:
            return []

        if fusion == "rrf":
            vec_rank = {rid: rank for rank, rid in enumerate(vec_hits.keys(), 1)}
            txt_rank = {rid: rank for rank, rid in enumerate(txt_hits.keys(), 1)}
            scored = [
                (rid, sum(1.0 / (rrf_k + r.get(rid, rrf_k * 10)) for r in [vec_rank, txt_rank]))
                for rid in ids
            ]
        else:
            scored = [
                (rid, vec_w * vec_hits.get(rid, 0.0) + txt_w * txt_hits.get(rid, 0.0))
                for rid in ids
            ]

        return sorted(scored, key=lambda x: x[1], reverse=True)[:limit]
