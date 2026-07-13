"""PostgreSQL storage models using Pydantic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypedDict

from contextunity.core.types import JsonDict
from pydantic import BaseModel, Field


class ScopePath(BaseModel):
    """Hierarchical memory scope using ltree-compatible path."""

    path: str = Field(..., description="ltree path, e.g. 'book.chapter_03'")


class GraphNode(BaseModel):
    """A node in the knowledge graph."""

    id: str
    content: str
    embedding: list[float] | None = None
    cell_kind: str = "concept"
    source_type: str | None = None
    source_id: str | None = None
    scope_path: str | None = None
    metadata: JsonDict = Field(default_factory=dict)
    title: str | None = None
    keywords_text: str | None = None
    content_hash: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None


class GraphEdge(BaseModel):
    """An edge connecting nodes in the knowledge graph."""

    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0
    metadata: JsonDict = Field(default_factory=dict)
    tenant_id: str | None = None


class SearchResult(BaseModel):
    """Result from hybrid search."""

    node: GraphNode
    score: float
    vector_score: float | None = None
    text_score: float | None = None
    connected_nodes: list[GraphNode] = Field(default_factory=list)


class GraphTraversalNode(TypedDict):
    """Node projection returned by structural graph traversal."""

    id: str
    cell_kind: str
    source_type: str
    title: str
    content: str
    scope_path: str
    metadata: JsonDict


class GraphTraversalEdge(TypedDict):
    """Edge projection returned by structural graph traversal."""

    source_id: str
    target_id: str
    relation: str
    weight: float
    depth: int


class GraphTraversalResult(TypedDict):
    """Result of ``graph_search`` — nodes and traversed edges."""

    nodes: list[GraphTraversalNode]
    edges: list[GraphTraversalEdge]


class BrainStorageInterface(ABC):
    """Abstract interface for knowledge storage."""

    @abstractmethod
    async def upsert_graph(
        self,
        nodes: list[GraphNode],
        edges: list[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None:
        """Insert or update LangGraph graph definitions.

        Args:
            nodes (List[GraphNode]): The nodes parameter.
            edges (List[GraphEdge]): The edges parameter.
        """

    @abstractmethod
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
    ) -> list[SearchResult]:
        """Perform hybrid (semantic + keyword) search over the vector storage.

        Returns:
            List[SearchResult]: A list of List[SearchResult].
        """


__all__ = [
    "ScopePath",
    "GraphNode",
    "GraphEdge",
    "GraphTraversalEdge",
    "GraphTraversalNode",
    "GraphTraversalResult",
    "SearchResult",
    "BrainStorageInterface",
]
