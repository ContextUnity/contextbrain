"""Secure graph serialization utilities.

This module provides secure alternatives to pickle for serializing NetworkX graphs
with integrity verification to prevent tampering.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TypeGuard

import networkx as nx
from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import SecurityError
from contextunity.core.narrowing import as_str, object_attr
from contextunity.core.types import is_json_dict

from contextunity.brain.core.exceptions import BrainGraphError
from contextunity.brain.ingestion.rag.protocols import import_module_object

logger = get_contextunit_logger(__name__)


def _joblib_dump(
    value: object,
    filename: object,
    *,
    compress: int,
    protocol: int,
) -> None:
    mod = import_module_object("joblib")
    dump_fn = object_attr(mod, "dump")
    if not callable(dump_fn):
        raise BrainGraphError("joblib.dump is not callable")
    _ = dump_fn(value, filename, compress=compress, protocol=protocol)


def _joblib_load(filename: object) -> object:
    mod = import_module_object("joblib")
    load_fn = object_attr(mod, "load")
    if not callable(load_fn):
        raise BrainGraphError("joblib.load is not callable")
    return load_fn(filename)


def _is_nx_graph(value: object) -> TypeGuard[nx.Graph[object]]:
    return isinstance(value, nx.Graph)


def _coerce_str_graph(loaded: object) -> nx.Graph[str]:
    """Rebuild a loaded graph with ``str`` node keys for strict typing."""
    if not _is_nx_graph(loaded):
        raise BrainGraphError(f"Expected networkx.Graph, got {type(loaded).__name__}")
    graph: nx.Graph[str] = nx.Graph()
    for node_obj in list(loaded.nodes()):
        _ = graph.add_node(str(node_obj))
    for edge in list(loaded.edges(data=True)):
        if len(edge) < 2:
            continue
        edge_tuple: tuple[object, ...] = edge
        u_obj = edge_tuple[0]
        v_obj = edge_tuple[1]
        data_obj: object = edge_tuple[2] if len(edge_tuple) > 2 else {}
        relation = "RELATED_TO"
        if is_json_dict(data_obj):
            relation = as_str(data_obj.get("relation"), default="RELATED_TO")
        _ = graph.add_edge(str(u_obj), str(v_obj), relation=relation)
    return graph


def _compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def save_graph_secure(
    graph: nx.Graph[str], file_path: Path, hash_file_path: Path | None = None
) -> None:
    """Save a graph securely with integrity verification.

    Args:
        graph: The graph object to save
        file_path: Path to save the graph file
        hash_file_path: Optional path to save the integrity hash (defaults to file_path + '.sha256')
    """
    if hash_file_path is None:
        hash_file_path = file_path.with_suffix(file_path.suffix + ".sha256")

    # Create parent directories
    file_path.parent.mkdir(parents=True, exist_ok=True)
    hash_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Save the graph using joblib (safer than pickle)
    _joblib_dump(graph, file_path, compress=True, protocol=4)

    # Compute and save integrity hash
    file_hash = _compute_file_hash(file_path)
    _ = hash_file_path.write_text(file_hash)

    logger.debug(
        "Graph saved securely to %s with integrity hash at %s",
        file_path,
        hash_file_path,
    )


def load_graph_secure(file_path: Path, hash_file_path: Path | None = None) -> nx.Graph[str]:
    """Load a graph securely with integrity verification.

    Args:
        file_path: Path to the graph file
        hash_file_path: Optional path to the integrity hash file (defaults to file_path + '.sha256')

    Returns:
        The loaded graph object

    Raises:
        ValueError: If integrity check fails or file doesn't exist
        FileNotFoundError: If graph file doesn't exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Graph file not found: {file_path}")

    if hash_file_path is None:
        hash_file_path = file_path.with_suffix(file_path.suffix + ".sha256")

    # Verify integrity if hash file exists
    if hash_file_path.exists():
        expected_hash = hash_file_path.read_text().strip()
        actual_hash = _compute_file_hash(file_path)

        if expected_hash != actual_hash:
            raise SecurityError(
                (
                    f"Integrity check failed for {file_path}. "
                    "File may have been tampered with. "
                    f"Expected hash: {expected_hash}, Actual hash: {actual_hash}"
                )
            )
    else:
        logger.warning(
            (
                f"No integrity hash file found for {file_path}. "
                "Loading without verification - this reduces security."
            )
        )

    # Load the graph using joblib
    try:
        loaded_obj = _joblib_load(file_path)
    except Exception as e:
        raise BrainGraphError(f"Failed to load graph from {file_path}: {e}") from e
    return _coerce_str_graph(loaded_obj)
