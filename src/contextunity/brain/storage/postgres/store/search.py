"""Hybrid search operations."""

from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from typing import override

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import StorageError
from contextunity.core.narrowing import as_float, as_str
from contextunity.core.types import is_json_dict
from psycopg import errors as pg_errors
from psycopg import sql
from psycopg.rows import dict_row

from ..models import GraphNode, ScopePath, SearchResult
from .base import PostgresStoreBase
from .helpers import PgConnection, vec

logger = get_contextunit_logger(__name__)


class SearchMixin(PostgresStoreBase, ABC):
    """Mixin for hybrid vector + text search."""

    @override
    async def hybrid_search(
        self,
        *,
        query_text: str,
        query_vec: list[float],
        tenant_id: str,
        candidate_k: int = 50,
        limit: int = 8,
        scope: ScopePath | None = None,
        source_types: list[str] | None = None,
        user_id: str | None = None,
        fusion: str = "weighted",
        rrf_k: int = 60,
        vector_weight: float = 0.8,
        text_weight: float = 0.2,
        **kwargs: object,
    ) -> list[SearchResult]:
        """Hybrid vector + text search with configurable fusion."""
        _ = kwargs
        if not tenant_id or candidate_k <= 0 or limit <= 0:
            return []

        async with await self.tenant_connection(tenant_id, user_id=user_id) as conn:
            where, params = self._build_scope_filters(
                tenant_id=tenant_id, user_id=user_id, scope=scope, source_types=source_types
            )
            where_sql = sql.SQL(" AND ").join(where)

            # Vector search
            vec_query = (
                sql.SQL(
                    (
                        "SELECT id, 1 - (embedding <=> %s::vector) AS score FROM cells"
                        " WHERE cell_kind = 'chunk' AND embedding IS NOT NULL AND "
                    )
                )
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
                    sql.SQL(
                        (
                            "SELECT id, ts_rank_cd("
                            "search_vector || COALESCE(keywords_vector, ''::tsvector),"
                            "websearch_to_tsquery('simple', %s)"
                            ") AS score FROM cells"
                            " WHERE cell_kind = 'chunk' AND ("
                            "search_vector || COALESCE(keywords_vector, ''::tsvector)"
                            ") @@ websearch_to_tsquery('simple', %s) AND "
                        )
                    )
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
        scope: ScopePath | None,
        source_types: list[str] | None,
    ) -> tuple[list[sql.SQL], list[object]]:
        """Build WHERE clause filters."""
        where = [sql.SQL("tenant_id = %s")]
        params: list[object] = [tenant_id]
        if user_id:
            where.append(sql.SQL("(user_id = %s OR user_id IS NULL)"))
            params.append(user_id)
        if scope:
            where.append(sql.SQL("scope_path <@ %s::ltree"))
            params.append(scope.path)
        if source_types:
            where.append(sql.SQL("source_type = ANY(%s::text[])"))
            params.append(source_types)
        return where, params

    async def _fetch_scores(
        self, conn: PgConnection, query: sql.Composed, params: list[object], key: str
    ) -> dict[str, float]:
        """Execute query and extract scores."""
        try:
            cur = conn.cursor(row_factory=dict_row)
            rows = await cur.execute(query, params)
        except pg_errors.UndefinedColumn as e:
            raise StorageError(f"Schema mismatch: {e}", code="SCHEMA_MISMATCH") from e
        except pg_errors.DatabaseError as e:
            raise StorageError(f"Query failed: {e}", code="DB_QUERY_ERROR") from e

        scores: dict[str, float] = {}
        async for raw_row in rows:
            if not is_json_dict(raw_row):
                continue
            score_cell = raw_row.get(key)
            if score_cell is None:
                continue
            scores[as_str(raw_row.get("id"))] = as_float(score_cell)
        return scores

    async def _fetch_nodes(
        self, conn: PgConnection, tenant_id: str, ids: Iterable[str]
    ) -> list[GraphNode]:
        """Fetch full node data."""
        cur = conn.cursor(row_factory=dict_row)
        rows = await cur.execute(
            """
            SELECT id, cell_kind, source_type, source_id, title, content,
                   struct_data, scope_path, tenant_id, user_id
            FROM cells WHERE tenant_id = %s AND id = ANY(%s::text[])
        """,
            [tenant_id, list(ids)],
        )

        nodes: list[GraphNode] = []
        async for raw_row in rows:
            if not is_json_dict(raw_row):
                continue
            struct_data = raw_row.get("struct_data")
            metadata = struct_data if is_json_dict(struct_data) else {}
            nodes.append(
                GraphNode(
                    id=as_str(raw_row.get("id")),
                    cell_kind=as_str(raw_row.get("cell_kind")),
                    content=as_str(raw_row.get("content")),
                    source_type=as_str(raw_row.get("source_type")) or None,
                    source_id=as_str(raw_row.get("source_id")) or None,
                    title=as_str(raw_row.get("title")) or None,
                    metadata=metadata,
                    scope_path=as_str(raw_row.get("scope_path")) or None,
                    tenant_id=as_str(raw_row.get("tenant_id")) or None,
                    user_id=as_str(raw_row.get("user_id")) or None,
                )
            )
        return nodes

    def _fuse_results(
        self,
        vec_hits: dict[str, float],
        txt_hits: dict[str, float],
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
