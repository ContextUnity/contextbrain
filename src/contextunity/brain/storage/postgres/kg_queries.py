"""Knowledge graph queries (Postgres)."""

from __future__ import annotations

from collections.abc import Iterable

from contextunity.core.narrowing import as_float, as_int, as_str
from contextunity.core.types import is_json_dict
from psycopg.rows import dict_row

from .models import GraphTraversalEdge, GraphTraversalNode, GraphTraversalResult
from .store.helpers import PgConnection


async def fetch_kg_facts(
    *,
    conn: PgConnection,
    tenant_id: str,
    entrypoints: Iterable[str],
    allowed_relations: list[str] | None,
    max_depth: int,
    max_facts: int,
) -> list[tuple[str, str, str]]:
    """Fetch kg facts.

    Returns:
        list[tuple[str, str, str]]: A list of list[tuple[str, str, str]].
    """
    if not entrypoints:
        return []
    cur = conn.cursor(row_factory=dict_row)
    sql = """
    WITH RECURSIVE walk AS (
        SELECT source_id, target_id, relation, 1 AS depth
        FROM cell_edges
        WHERE tenant_id = %s
          AND source_id = ANY(%s::text[])
          AND (%s::text[] IS NULL OR relation = ANY(%s::text[]))
        UNION ALL
        SELECT e.source_id, e.target_id, e.relation, w.depth + 1
        FROM walk w
        JOIN cell_edges e ON e.source_id = w.target_id
        WHERE w.depth < %s
          AND e.tenant_id = %s
          AND (%s::text[] IS NULL OR e.relation = ANY(%s::text[]))
    )
    SELECT source_id, target_id, relation
    FROM walk
    LIMIT %s
    """
    params = [
        tenant_id,
        list(entrypoints),
        allowed_relations,
        allowed_relations,
        max_depth,
        tenant_id,
        allowed_relations,
        allowed_relations,
        max_facts,
    ]
    rows = await cur.execute(sql, params)
    out: list[tuple[str, str, str]] = []
    async for raw_row in rows:
        if not is_json_dict(raw_row):
            continue
        out.append(
            (
                as_str(raw_row.get("source_id")),
                as_str(raw_row.get("target_id")),
                as_str(raw_row.get("relation")),
            )
        )
    return out


async def graph_search(
    *,
    conn: PgConnection,
    tenant_id: str,
    entrypoint_ids: list[str],
    allowed_relations: list[str] | None,
    max_hops: int,
    max_results: int = 200,
) -> GraphTraversalResult:
    """Structural graph traversal returning nodes and edges.

    Uses recursive CTE to walk the cell_edges table starting from
    entrypoint_ids. Returns discovered nodes with their attributes and
    the edges traversed.

    Returns:
        Dict with 'nodes' and 'edges' lists.
    """
    if not entrypoint_ids:
        return {"nodes": [], "edges": []}

    cur = conn.cursor(row_factory=dict_row)

    # Step 1: Recursive edge traversal
    edge_sql = """
    WITH RECURSIVE walk AS (
        SELECT source_id, target_id, relation, weight, metadata, 1 AS depth
        FROM cell_edges
        WHERE tenant_id = %s
          AND source_id = ANY(%s::text[])
          AND (%s::text[] IS NULL OR relation = ANY(%s::text[]))
        UNION ALL
        SELECT e.source_id, e.target_id, e.relation, e.weight, e.metadata, w.depth + 1
        FROM walk w
        JOIN cell_edges e ON e.source_id = w.target_id
        WHERE w.depth < %s
          AND e.tenant_id = %s
          AND (%s::text[] IS NULL OR e.relation = ANY(%s::text[]))
    )
    SELECT DISTINCT source_id, target_id, relation, weight, metadata, depth
    FROM walk
    ORDER BY depth, source_id
    LIMIT %s
    """
    relations_param = allowed_relations if allowed_relations else None
    edge_params = [
        tenant_id,
        list(entrypoint_ids),
        relations_param,
        relations_param,
        max_hops,
        tenant_id,
        relations_param,
        relations_param,
        max_results,
    ]

    edge_rows = await cur.execute(edge_sql, edge_params)
    edges: list[GraphTraversalEdge] = []
    node_ids: set[str] = set(entrypoint_ids)

    async for raw_row in edge_rows:
        if not is_json_dict(raw_row):
            continue
        source_id = as_str(raw_row.get("source_id"))
        target_id = as_str(raw_row.get("target_id"))
        edges.append(
            {
                "source_id": source_id,
                "target_id": target_id,
                "relation": as_str(raw_row.get("relation")),
                "weight": as_float(raw_row.get("weight"), default=1.0),
                "depth": as_int(raw_row.get("depth"), default=1),
            }
        )
        node_ids.add(source_id)
        node_ids.add(target_id)

    if not node_ids:
        return {"nodes": [], "edges": edges}

    # Step 2: Fetch node attributes for all referenced nodes
    node_sql = """
    SELECT id, node_kind, source_type, source_id, title, content,
           struct_data, scope_path, tenant_id
    FROM cells
    WHERE tenant_id = %s AND id = ANY(%s::text[])
    """
    node_rows = await cur.execute(node_sql, [tenant_id, list(node_ids)])

    nodes: list[GraphTraversalNode] = []
    async for raw_row in node_rows:
        if not is_json_dict(raw_row):
            continue
        content = as_str(raw_row.get("content"))
        struct_data = raw_row.get("struct_data")
        metadata = struct_data if is_json_dict(struct_data) else {}
        nodes.append(
            {
                "id": as_str(raw_row.get("id")),
                "node_kind": as_str(raw_row.get("node_kind")),
                "source_type": as_str(raw_row.get("source_type")),
                "title": as_str(raw_row.get("title")),
                "content": content[:500],
                "scope_path": as_str(raw_row.get("scope_path")),
                "metadata": metadata,
            }
        )

    return {"nodes": nodes, "edges": edges}


__all__ = ["fetch_kg_facts", "graph_search"]
