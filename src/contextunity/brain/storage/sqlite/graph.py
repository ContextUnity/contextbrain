"""Knowledge graph storage (SQLite implementation).

Contract-compatible with ``postgres/store/graph.py``.
Uses ``entrypoint_ids`` (not ``source_id``) for ``graph_search``.
"""

from __future__ import annotations

import sqlite3

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_float, as_str
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.postgres.models import (
    GraphEdge,
    GraphNode,
    GraphTraversalEdge,
    GraphTraversalNode,
    GraphTraversalResult,
)

from .codecs import json_dumps, json_loads, sqlite_cell, vec_to_bytes

logger = get_contextunit_logger(__name__)
from .connection import SqliteConnectionMixin  # noqa: E402


class GraphMixin(SqliteConnectionMixin):
    """SQLite graph operations matching Postgres contract."""

    async def upsert_graph(
        self,
        nodes: list[GraphNode],
        edges: list[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None:
        """Upsert knowledge graph nodes and edges."""
        if not tenant_id:
            raise BrainValidationError("tenant_id is required")

        with self._get_connection() as db:
            for node in nodes:
                _ = db.execute(
                    """
                    INSERT INTO knowledge_nodes (
                        id, tenant_id, user_id, node_kind, source_type, source_id,
                        title, content, struct_data, keywords_text, taxonomy_path,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT (id) DO UPDATE SET
                        title = excluded.title,
                        content = excluded.content,
                        struct_data = excluded.struct_data,
                        keywords_text = excluded.keywords_text,
                        taxonomy_path = excluded.taxonomy_path,
                        updated_at = datetime('now')
                    """,
                    (
                        node.id,
                        tenant_id,
                        user_id,
                        node.node_kind,
                        node.source_type,
                        node.source_id,
                        node.title,
                        node.content,
                        json_dumps(node.metadata),
                        node.keywords_text,
                        node.taxonomy_path,
                    ),
                )

                # Upsert embedding into vec table if available
                if node.embedding and self.has_sqlite_vec():
                    _ = db.execute(
                        """
                        INSERT INTO vec_knowledge_nodes (node_id, embedding)
                        VALUES (?, ?)
                        ON CONFLICT (node_id) DO UPDATE SET
                            embedding = excluded.embedding
                        """,
                        (node.id, vec_to_bytes(node.embedding)),
                    )

            for edge in edges:
                _ = db.execute(
                    """
                    INSERT INTO knowledge_edges
                        (tenant_id, source_id, target_id, relation, weight, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (tenant_id, source_id, target_id, relation) DO UPDATE SET
                        weight = excluded.weight,
                        metadata = excluded.metadata
                    """,
                    (
                        tenant_id,
                        edge.source_id,
                        edge.target_id,
                        edge.relation,
                        edge.weight,
                        json_dumps(edge.metadata),
                    ),
                )
            db.commit()

    async def graph_search(
        self,
        *,
        tenant_id: str,
        user_id: str | None = None,
        entrypoint_ids: list[str],
        max_hops: int = 2,
        allowed_relations: list[str] | None = None,
        max_results: int = 200,
    ) -> GraphTraversalResult:
        """Structural graph traversal from entrypoint_ids.

        SQLite does not support recursive CTEs as efficiently as Postgres,
        so we implement iterative BFS up to ``max_hops``.

        Returns dict with 'nodes' and 'edges' lists.
        """
        _ = user_id
        if not entrypoint_ids:
            return {"nodes": [], "edges": []}

        with self._get_connection() as db:
            edges: list[GraphTraversalEdge] = []
            visited_edges: set[tuple[str, str, str]] = set()
            frontier = set(entrypoint_ids)
            all_node_ids = set(entrypoint_ids)

            for hop in range(max_hops):
                if not frontier:
                    break

                placeholders = ", ".join("?" for _ in frontier)
                params: list[object] = [tenant_id, *frontier]

                rel_filter = ""
                if allowed_relations:
                    rel_placeholders = ", ".join("?" for _ in allowed_relations)
                    rel_filter = f" AND relation IN ({rel_placeholders})"
                    params.extend(allowed_relations)

                params.append(max_results - len(edges))

                cursor = db.execute(
                    f"""
                    SELECT source_id, target_id, relation, weight
                    FROM knowledge_edges
                    WHERE tenant_id = ?
                      AND source_id IN ({placeholders})
                      {rel_filter}
                    LIMIT ?
                    """,
                    params,
                )

                next_frontier: set[str] = set()
                edge_rows: list[sqlite3.Row] = list(cursor.fetchall())
                for row in edge_rows:
                    source_id = as_str(sqlite_cell(row, "source_id"))
                    target_id = as_str(sqlite_cell(row, "target_id"))
                    relation = as_str(sqlite_cell(row, "relation"))
                    edge_key = (source_id, target_id, relation)
                    if edge_key in visited_edges:
                        continue
                    visited_edges.add(edge_key)
                    edges.append(
                        {
                            "source_id": source_id,
                            "target_id": target_id,
                            "relation": relation,
                            "weight": as_float(sqlite_cell(row, "weight"), default=1.0),
                            "depth": hop + 1,
                        }
                    )
                    all_node_ids.add(source_id)
                    all_node_ids.add(target_id)
                    next_frontier.add(target_id)

                frontier = next_frontier - all_node_ids | next_frontier

                if len(edges) >= max_results:
                    break

            # Fetch node attributes
            if not all_node_ids:
                return {"nodes": [], "edges": edges}

            node_placeholders = ", ".join("?" for _ in all_node_ids)
            cursor = db.execute(
                f"""
                SELECT id, node_kind, source_type, source_id, title,
                       substr(content, 1, 500) as content,
                       struct_data, taxonomy_path, tenant_id
                FROM knowledge_nodes
                WHERE tenant_id = ? AND id IN ({node_placeholders})
                """,
                [tenant_id, *all_node_ids],
            )

            nodes: list[GraphTraversalNode] = []
            node_rows: list[sqlite3.Row] = list(cursor.fetchall())
            for row in node_rows:
                struct_cell = sqlite_cell(row, "struct_data")
                meta_raw = json_loads(struct_cell if isinstance(struct_cell, str) else None)
                metadata: JsonDict = meta_raw if is_json_dict(meta_raw) else {}
                nodes.append(
                    {
                        "id": as_str(sqlite_cell(row, "id")),
                        "node_kind": as_str(sqlite_cell(row, "node_kind")),
                        "source_type": as_str(sqlite_cell(row, "source_type")),
                        "title": as_str(sqlite_cell(row, "title")),
                        "content": as_str(sqlite_cell(row, "content")),
                        "taxonomy_path": as_str(sqlite_cell(row, "taxonomy_path")),
                        "metadata": metadata,
                    }
                )

        return {"nodes": nodes, "edges": edges}


__all__ = ["GraphMixin"]
