"""Stage: Cognee KG extraction (nodes + edges JSONL)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_float
from contextunity.core.types import JsonDict

from contextunity.brain.storage.graph.cognee import KnowledgeGraphOrchestrator

from ..config import get_assets_paths
from ..core.utils import parallel_map, resolve_workers
from ..settings import RagIngestionConfig
from ..stages.store import read_raw_data_jsonl

logger = get_contextunit_logger(__name__)


def extract_cognee_kg(
    *,
    config: RagIngestionConfig,
    only_types: list[str],
    overwrite: bool = True,
    workers: int = 1,
) -> dict[str, str]:
    """Extract cognee kg.

    Returns:
        dict[str, str]: A dictionary containing the results.
    """
    paths = get_assets_paths(config)
    nodes_path = paths["assets"] / "knowledge_nodes.jsonl"
    edges_path = paths["assets"] / "knowledge_edges.jsonl"

    if overwrite:
        for p in (nodes_path, edges_path):
            if p.exists():
                p.unlink()

    builder = KnowledgeGraphOrchestrator()
    if not builder.is_available():
        logger.warning("Cognee not available; KG extraction skipped")
        return {"nodes_path": str(nodes_path), "edges_path": str(edges_path)}

    def _run_one(t: str) -> tuple[list[JsonDict], list[JsonDict]]:
        """run one.

        Args:
            t (str): The t parameter.

        Returns:
            tuple[list[dict], list[dict]]: A list of tuple[list[dict], list[dict]].
        """
        clean_path = paths["clean_text"] / f"{t}.jsonl"
        records = read_raw_data_jsonl(clean_path)
        nodes: list[JsonDict] = []
        edges: list[JsonDict] = []
        for rec in records:
            import asyncio

            result = asyncio.run(builder.build_graph(rec.content or ""))
            if result is None:
                continue
            entities, relations = result
            for ent in entities:
                name = str(ent.get("name") or ent.get("id") or "").strip()
                if not name:
                    continue
                node_id = _stable_id("concept", name)
                nodes.append(
                    {
                        "id": node_id,
                        "node_kind": "concept",
                        "content": name,
                        "struct_data": {"source_type": "knowledge"},
                    }
                )
            for rel in relations:
                src = str(rel.get("source") or "").strip()
                tgt = str(rel.get("target") or "").strip()
                relation = str(rel.get("relation") or "relates_to").strip()
                if not src or not tgt:
                    continue
                edges.append(
                    {
                        "source_id": _stable_id("concept", src),
                        "target_id": _stable_id("concept", tgt),
                        "relation": relation,
                        "weight": as_float(rel.get("weight"), default=1.0),
                        "metadata": {},
                    }
                )
        return nodes, edges

    w = resolve_workers(config=config, workers=workers)
    results = parallel_map(only_types, _run_one, workers=w, ordered=False, swallow_exceptions=False)
    all_nodes: list[JsonDict] = []
    all_edges: list[JsonDict] = []
    for r in results:
        if not r:
            continue
        nodes, edges = r
        all_nodes.extend(nodes)
        all_edges.extend(edges)

    _write_jsonl(nodes_path, all_nodes)
    _write_jsonl(edges_path, all_edges)
    logger.info("cognee_kg: nodes=%d edges=%d", len(all_nodes), len(all_edges))
    return {"nodes_path": str(nodes_path), "edges_path": str(edges_path)}


def _write_jsonl(path: Path, rows: list[JsonDict]) -> None:
    """write jsonl.

    Args:
        path (Path): The filesystem path.
        rows (list[dict]): The rows parameter.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            _ = f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _stable_id(prefix: str, value: str) -> str:
    """stable id.

    Args:
        prefix (str): The prefix parameter.
        value (str): The value to store or update.

    Returns:
        str: The resulting string value.
    """
    h = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{prefix}_{h[:16]}"


__all__ = ["extract_cognee_kg"]
