"""Stage: Load KG nodes/edges/aliases into Postgres."""

from __future__ import annotations

import asyncio
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.narrowing import as_float, as_json_dict, as_str
from contextunity.core.parsing import json_loads
from contextunity.core.types import JsonDict, is_json_dict
from psycopg_pool import AsyncConnectionPool

from contextunity.brain.core import get_core_config
from contextunity.brain.storage.postgres import GraphEdge, GraphNode, PostgresKnowledgeStore

from ..config import get_assets_paths
from ..settings import RagIngestionConfig

logger = get_contextunit_logger(__name__)


def load_postgres_kg(*, config: RagIngestionConfig) -> dict[str, str]:
    """Load postgres kg.

    Returns:
        dict[str, str]: A dictionary containing the results.
    """
    return asyncio.run(_load_async(config=config))


async def _load_async(*, config: RagIngestionConfig) -> dict[str, str]:
    """load async.

    Returns:
        dict[str, str]: A dictionary containing the results.

    Raises:
        ValueError: If parameter values are invalid.
    """
    core_cfg = get_core_config()
    dsn = config.upload.postgres.dsn or core_cfg.postgres.dsn
    if not dsn:
        raise ConfigurationError("Postgres DSN is required for KG load")
    tenant_id = config.upload.postgres.tenant_id or "public"
    user_id = config.upload.postgres.user_id

    paths = get_assets_paths(config)
    nodes_path = paths["assets"] / "knowledge_nodes.jsonl"
    edges_path = paths["assets"] / "knowledge_edges.jsonl"
    aliases_path = paths["assets"] / "knowledge_aliases.jsonl"

    nodes = _load_nodes(nodes_path)
    edges = _load_edges(edges_path)

    store = PostgresKnowledgeStore(
        dsn=dsn,
        pool_min_size=config.upload.postgres.pool_min_size,
        pool_max_size=config.upload.postgres.pool_max_size,
    )
    await store.upsert_graph(nodes, edges, tenant_id=str(tenant_id), user_id=user_id)

    if aliases_path.exists():
        await _load_aliases(
            dsn=dsn,
            tenant_id=str(tenant_id),
            aliases=_load_jsonl(aliases_path),
        )

    return {
        "nodes_path": str(nodes_path),
        "edges_path": str(edges_path),
        "aliases_path": str(aliases_path),
    }


def _load_nodes(path: Path) -> list[GraphNode]:
    """load nodes.

    Args:
        path (Path): The filesystem path.

    Returns:
        list[GraphNode]: A list of list[GraphNode].
    """
    rows = _load_jsonl(path)
    nodes: list[GraphNode] = []
    for row in rows:
        nodes.append(
            GraphNode(
                id=as_str(row.get("id")),
                content=as_str(row.get("content")),
                node_kind=as_str(row.get("node_kind"), default="concept"),
                source_type=as_str(row.get("source_type")) or None,
                source_id=as_str(row.get("source_id")) or None,
                title=as_str(row.get("title")) or None,
                taxonomy_path=as_str(row.get("taxonomy_path")) or None,
                metadata=as_json_dict(row.get("struct_data")),
            )
        )
    return nodes


def _load_edges(path: Path) -> list[GraphEdge]:
    """load edges.

    Args:
        path (Path): The filesystem path.

    Returns:
        list[GraphEdge]: A list of list[GraphEdge].
    """
    rows = _load_jsonl(path)
    edges: list[GraphEdge] = []
    for row in rows:
        edges.append(
            GraphEdge(
                source_id=as_str(row.get("source_id")),
                target_id=as_str(row.get("target_id")),
                relation=as_str(row.get("relation"), default="relates_to"),
                weight=as_float(row.get("weight"), default=1.0),
                metadata=as_json_dict(row.get("metadata")),
            )
        )
    return edges


async def _load_aliases(*, dsn: str, tenant_id: str, aliases: list[JsonDict]) -> None:
    """load aliases."""
    async with AsyncConnectionPool(dsn, min_size=2, max_size=10) as pool:
        async with pool.connection() as conn:
            async with conn.transaction():
                for row in aliases:
                    alias = str(row.get("alias") or "").strip()
                    node_id = str(row.get("node_id") or "").strip()
                    source = str(row.get("source") or "").strip()
                    if not alias or not node_id or not source:
                        continue
                    _ = await conn.execute(
                        """
                        INSERT INTO knowledge_aliases (tenant_id, alias, node_id, source)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (tenant_id, alias) DO UPDATE
                        SET node_id = EXCLUDED.node_id,
                            source = EXCLUDED.source
                        """,
                        [tenant_id, alias, node_id, source],
                    )


def _load_jsonl(path: Path) -> list[JsonDict]:
    """load jsonl.

    Args:
        path (Path): The filesystem path.

    Returns:
        list[JsonDict]: A list of list[JsonDict].
    """
    if not path.exists():
        return []
    rows: list[JsonDict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json_loads(line)
        except Exception as e:
            logger.debug("Failed to decode JSON line: %s", e)
            continue
        if is_json_dict(row):
            rows.append(row)
    return rows


__all__ = ["load_postgres_kg"]
