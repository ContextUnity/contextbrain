"""PostgreSQL storage models using Pydantic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class TaxonomyPath(BaseModel):
    """Hierarchical taxonomy scope using ltree-compatible path."""

    path: str = Field(..., description="ltree path, e.g. 'book.chapter_03'")


class GraphNode(BaseModel):
    """A node in the knowledge graph."""

    id: str
    content: str
    embedding: List[float] | None = None
    node_kind: str = "concept"
    source_type: str | None = None
    source_id: str | None = None
    taxonomy_path: str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    title: str | None = None
    keywords_text: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None


class GraphEdge(BaseModel):
    """An edge connecting nodes in the knowledge graph."""

    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None


class SearchResult(BaseModel):
    """Result from hybrid search."""

    node: GraphNode
    score: float
    vector_score: float | None = None
    text_score: float | None = None
    connected_nodes: List[GraphNode] = Field(default_factory=list)


class KnowledgeStoreInterface(ABC):
    """Abstract interface for knowledge storage."""

    @abstractmethod
    async def upsert_graph(
        self,
        nodes: List[GraphNode],
        edges: List[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def hybrid_search(
        self,
        *,
        query_text: str,
        query_vec: List[float],
        tenant_id: str,
        limit: int = 8,
        **kwargs,
    ) -> List[SearchResult]: ...


__all__ = ["TaxonomyPath", "GraphNode", "GraphEdge", "SearchResult", "KnowledgeStoreInterface"]
