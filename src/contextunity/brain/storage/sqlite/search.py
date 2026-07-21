"""Hybrid search (SQLite + sqlite-vec implementation).

Contract-compatible with ``postgres/store/search.py``.
Uses sqlite-vec for vector distance and LIKE for text fallback.
Returns ``SearchResult`` models in the same shape as Postgres.
"""

from __future__ import annotations

import sqlite3

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_float, as_str
from contextunity.core.types import is_json_dict

from contextunity.brain.storage.postgres.models import GraphNode, ScopePath, SearchResult

from .codecs import json_loads, sqlite_cell, vec_to_bytes
from .connection import SqliteConnectionMixin

logger = get_contextunit_logger(__name__)


class SearchMixin(SqliteConnectionMixin):
    """SQLite hybrid search matching Postgres contract."""

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
        metadata_filter: dict[str, str] | None = None,
        fusion: str = "weighted",
        rrf_k: int = 60,
        vector_weight: float = 0.8,
        text_weight: float = 0.2,
        **_: object,
    ) -> list[SearchResult]:
        """Hybrid vector + text search with configurable fusion.

        Local approximation:
        - Vector search via sqlite-vec ``vec_distance_L2``
        - Text search via SQLite ``LIKE`` (no tsvector/trigrams)
        - Results fused using weighted sum or RRF
        """
        if not tenant_id or candidate_k <= 0 or limit <= 0:
            return []

        vec_hits: dict[str, float] = {}
        text_hits: dict[str, float] = {}

        with self._get_connection() as db:
            # Vector search (if sqlite-vec available)
            if self.has_sqlite_vec() and query_vec:
                vec_params: list[object] = [vec_to_bytes(query_vec), candidate_k]
                cursor = db.execute(
                    """
                    SELECT node_id, distance
                    FROM vec_cells
                    WHERE embedding MATCH ?
                    ORDER BY distance
                    LIMIT ?
                    """,
                    vec_params,
                )
                vec_rows: list[sqlite3.Row] = list(cursor.fetchall())
                for row in vec_rows:
                    node_id = as_str(sqlite_cell(row, "node_id"))
                    dist_cell = sqlite_cell(row, "distance")
                    dist = float(dist_cell) if isinstance(dist_cell, (int, float)) else 0.0
                    vec_hits[node_id] = 1.0 / (1.0 + dist)

                # Post-filter by tenant/scope/source_type
                if vec_hits:
                    node_placeholders = ", ".join("?" for _ in vec_hits)
                    filter_q = f"""
                        SELECT id FROM cells
                        WHERE tenant_id = ? AND id IN ({node_placeholders})
                    """
                    filter_params: list[object] = [tenant_id, *vec_hits.keys()]

                    if user_id:
                        filter_q += (
                            " AND (user_id = ? OR (user_id IS NULL AND visibility <> 'private'))"
                        )
                        filter_params.append(user_id)
                    else:
                        filter_q += " AND visibility <> 'private'"
                    if source_types:
                        st_ph = ", ".join("?" for _ in source_types)
                        filter_q += f" AND source_type IN ({st_ph})"
                        filter_params.extend(source_types)
                    if scope:
                        filter_q += " AND (scope_path = ? OR substr(scope_path, 1, length(?) + 1) = ? || '.')"
                        filter_params.extend([scope.path, scope.path, scope.path])
                    for key, value in sorted((metadata_filter or {}).items()):
                        filter_q += " AND json_extract(struct_data, ?) = ?"
                        filter_params.extend([f"$.{key}", value])

                    cursor = db.execute(filter_q, filter_params)
                    filter_rows: list[sqlite3.Row] = list(cursor.fetchall())
                    valid_ids = {as_str(sqlite_cell(row, "id")) for row in filter_rows}
                    vec_hits = {k: v for k, v in vec_hits.items() if k in valid_ids}

            # Text search (LIKE fallback)
            if query_text and query_text.strip():
                text_q = """
                    SELECT id FROM cells
                    WHERE tenant_id = ? AND cell_kind = 'chunk'
                      AND (content LIKE ? OR keywords_text LIKE ? OR title LIKE ?)
                """
                like_pat = f"%{query_text.strip()}%"
                text_params: list[object] = [tenant_id, like_pat, like_pat, like_pat]

                if user_id:
                    text_q += " AND (user_id = ? OR (user_id IS NULL AND visibility <> 'private'))"
                    text_params.append(user_id)
                else:
                    text_q += " AND visibility <> 'private'"
                if source_types:
                    source_placeholders = ", ".join("?" for _ in source_types)
                    text_q += f" AND source_type IN ({source_placeholders})"
                    text_params.extend(source_types)
                if scope:
                    text_q += (
                        " AND (scope_path = ? OR substr(scope_path, 1, length(?) + 1) = ? || '.')"
                    )
                    text_params.extend([scope.path, scope.path, scope.path])
                for key, value in sorted((metadata_filter or {}).items()):
                    text_q += " AND json_extract(struct_data, ?) = ?"
                    text_params.extend([f"$.{key}", value])

                text_q += " LIMIT ?"
                text_params.append(candidate_k)

                cursor = db.execute(text_q, text_params)
                text_rows: list[sqlite3.Row] = list(cursor.fetchall())
                for rank, row in enumerate(text_rows, 1):
                    text_hits[as_str(sqlite_cell(row, "id"))] = 1.0 / rank

            # Fuse results
            all_ids = set(vec_hits) | set(text_hits)
            if not all_ids:
                return []

            if fusion == "rrf":
                vec_rank = {rid: rank for rank, rid in enumerate(vec_hits.keys(), 1)}
                txt_rank = {rid: rank for rank, rid in enumerate(text_hits.keys(), 1)}
                scored = [
                    (rid, sum(1.0 / (rrf_k + r.get(rid, rrf_k * 10)) for r in [vec_rank, txt_rank]))
                    for rid in all_ids
                ]
            else:
                scored = [
                    (
                        rid,
                        vector_weight * vec_hits.get(rid, 0.0)
                        + text_weight * text_hits.get(rid, 0.0),
                    )
                    for rid in all_ids
                ]

            ranked = sorted(scored, key=lambda x: x[1], reverse=True)[:limit]

            # Fetch node data
            ranked_ids = [r[0] for r in ranked]
            node_placeholders = ", ".join("?" for _ in ranked_ids)
            cursor = db.execute(
                f"""
                SELECT id, cell_kind, source_type, source_id, source_ref, title,
                       content, struct_data, scope_path, content_hash, confidence,
                       visibility, tenant_id, user_id
                FROM cells
                WHERE tenant_id = ? AND id IN ({node_placeholders})
                """,
                [tenant_id, *ranked_ids],
            )

            node_map: dict[str, GraphNode] = {}
            node_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in node_rows:
                struct_cell = sqlite_cell(row, "struct_data")
                meta = json_loads(struct_cell if isinstance(struct_cell, str) else None)
                node_id = as_str(sqlite_cell(row, "id"))
                node_map[node_id] = GraphNode(
                    id=node_id,
                    cell_kind=as_str(sqlite_cell(row, "cell_kind")),
                    content=as_str(sqlite_cell(row, "content")),
                    source_type=as_str(sqlite_cell(row, "source_type")) or None,
                    source_id=as_str(sqlite_cell(row, "source_id")) or None,
                    source_ref=as_str(sqlite_cell(row, "source_ref")) or None,
                    title=as_str(sqlite_cell(row, "title")) or None,
                    metadata=meta if is_json_dict(meta) else {},
                    scope_path=as_str(sqlite_cell(row, "scope_path")) or None,
                    content_hash=as_str(sqlite_cell(row, "content_hash")) or None,
                    confidence=as_float(sqlite_cell(row, "confidence"), default=0.5),
                    visibility=as_str(sqlite_cell(row, "visibility"), default="tenant"),
                    tenant_id=as_str(sqlite_cell(row, "tenant_id")) or None,
                    user_id=as_str(sqlite_cell(row, "user_id")) or None,
                )

        return [
            SearchResult(
                node=node_map[rid],
                score=score,
                vector_score=vec_hits.get(rid),
                text_score=text_hits.get(rid),
            )
            for rid, score in ranked
            if rid in node_map
        ]


__all__ = ["SearchMixin"]
