"""Knowledge graph queries (Postgres)."""

from __future__ import annotations

from typing import Iterable

from psycopg.rows import dict_row


async def fetch_kg_facts(
    *,
    conn,
    tenant_id: str,
    entrypoints: Iterable[str],
    allowed_relations: list[str] | None,
    max_depth: int,
    max_facts: int,
) -> list[tuple[str, str, str]]:
    if not entrypoints:
        return []
    conn.row_factory = dict_row
    sql = """
    WITH RECURSIVE walk AS (
        SELECT source_id, target_id, relation, 1 AS depth
        FROM knowledge_edges
        WHERE tenant_id = %s
          AND source_id = ANY(%s::text[])
          AND (%s::text[] IS NULL OR relation = ANY(%s::text[]))
        UNION ALL
        SELECT e.source_id, e.target_id, e.relation, w.depth + 1
        FROM walk w
        JOIN knowledge_edges e ON e.source_id = w.target_id
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
    rows = await conn.execute(sql, params)
    out: list[tuple[str, str, str]] = []
    async for row in rows:
        out.append((row["source_id"], row["target_id"], row["relation"]))
    return out


async def graph_search(
    *,
    conn,
    tenant_id: str,
    entrypoint_ids: list[str],
    allowed_relations: list[str] | None,
    max_hops: int,
    max_results: int = 200,
) -> dict:
    """Structural graph traversal returning nodes and edges.

    Uses recursive CTE to walk the knowledge_edges table starting from
    entrypoint_ids. Returns discovered nodes with their attributes and
    the edges traversed.

    Returns:
        Dict with 'nodes' and 'edges' lists.
    """
    if not entrypoint_ids:
        return {"nodes": [], "edges": []}

    conn.row_factory = dict_row

    # Step 1: Recursive edge traversal
    edge_sql = """
    WITH RECURSIVE walk AS (
        SELECT source_id, target_id, relation, weight, metadata, 1 AS depth
        FROM knowledge_edges
        WHERE tenant_id = %s
          AND source_id = ANY(%s::text[])
          AND (%s::text[] IS NULL OR relation = ANY(%s::text[]))
        UNION ALL
        SELECT e.source_id, e.target_id, e.relation, e.weight, e.metadata, w.depth + 1
        FROM walk w
        JOIN knowledge_edges e ON e.source_id = w.target_id
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

    edge_rows = await conn.execute(edge_sql, edge_params)
    edges: list[dict] = []
    node_ids: set[str] = set(entrypoint_ids)

    async for row in edge_rows:
        edges.append(
            {
                "source_id": row["source_id"],
                "target_id": row["target_id"],
                "relation": row["relation"],
                "weight": float(row.get("weight", 1.0) or 1.0),
                "depth": row.get("depth", 1),
            }
        )
        node_ids.add(row["source_id"])
        node_ids.add(row["target_id"])

    if not node_ids:
        return {"nodes": [], "edges": edges}

    # Step 2: Fetch node attributes for all referenced nodes
    node_sql = """
    SELECT id, node_kind, source_type, source_id, title, content,
           struct_data, taxonomy_path, tenant_id
    FROM knowledge_nodes
    WHERE tenant_id = %s AND id = ANY(%s::text[])
    """
    node_rows = await conn.execute(node_sql, [tenant_id, list(node_ids)])

    nodes: list[dict] = []
    async for row in node_rows:
        nodes.append(
            {
                "id": row["id"],
                "node_kind": row.get("node_kind", ""),
                "source_type": row.get("source_type", ""),
                "title": row.get("title", ""),
                "content": (row.get("content") or "")[:500],  # Truncate for response size
                "taxonomy_path": row.get("taxonomy_path", ""),
                "metadata": row.get("struct_data") or {},
            }
        )

    return {"nodes": nodes, "edges": edges}


__all__ = ["fetch_kg_facts", "graph_search"]
