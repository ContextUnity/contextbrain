"""Knowledge graph and embedding-job storage protocols."""

from __future__ import annotations

from typing import Protocol

from contextunity.core.types import JsonDict

from contextunity.brain.storage.postgres.models import (
    GraphEdge,
    GraphNode,
    GraphTraversalResult,
    ScopePath,
    SearchResult,
)


class KnowledgeStorageProtocol(Protocol):
    # ── Knowledge Graph ───────────────────────────────────────────
    # Handlers: CellSearchHandlersMixin and CellEdgeHandlersMixin

    async def upsert_graph(
        self,
        nodes: list[GraphNode],
        edges: list[GraphEdge],
        *,
        tenant_id: str,
        user_id: str | None = None,
    ) -> None:
        """Insert or update entity nodes and relations in the Knowledge Graph.

        Performs a transactionally safe upsert of structural knowledge.

        Args:
            nodes: List of structured entities/nodes.
            edges: List of relation triples/edges.
            tenant_id: Tenant partition owner.
            user_id: Optional user owner identifier.
        """
        ...

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
        """Perform a graph traversal query starting from specific root entity IDs.

        Finds connected nodes and relationships within a specified depth limit.

        Args:
            tenant_id: Tenant partition boundary.
            user_id: Optional user owner filter.
            entrypoint_ids: Root entity node IDs to begin search from.
            max_hops: Maximum traversal path length. Defaults to 2.
            allowed_relations: Optional list of relationship names to filter edges.
            max_results: Upper bound on the number of returned items. Defaults to 200.

        Returns:
            A dictionary containing lists of matching "nodes" and "edges".
        """
        ...

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
        """Execute a unified search combining vector similarity and keyword search.

        Synthesizes vector distance (semantic search) and full-text indexes
        using Reciprocal Rank Fusion (RRF) or Weighted Scoring.

        Args:
            query_text: Natural language search string.
            query_vec: Dense embedding vector representing the search query.
            tenant_id: Tenant partition to query.
            candidate_k: Number of semantic matches to fetch for rank fusion. Defaults to 50.
            limit: Maximum search results to return. Defaults to 8.
            scope: Optional scoping metadata filter.
            source_types: Optional list of entity/record types to include.
            user_id: Optional user filter for personalized search.
            fusion: Scoring strategy, e.g. "weighted" or "rrf". Defaults to "weighted".
            rrf_k: Reciprocal Rank Fusion constant parameter. Defaults to 60.
            vector_weight: Weight applied to semantic similarity. Defaults to 0.8.
            text_weight: Weight applied to text match scores. Defaults to 0.2.

        Returns:
            A list of matched documents/records sorted by combined rank.
        """
        ...

    async def enqueue_embedding_job(
        self,
        *,
        tenant_id: str,
        cell_id: str,
        content_hash: str,
        profile: str,
        max_pending: int,
    ) -> JsonDict: ...

    async def claim_embedding_jobs(
        self, *, tenant_id: str, limit: int, lease_seconds: int
    ) -> list[JsonDict]: ...

    async def complete_embedding_job(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
        vector: list[float],
    ) -> JsonDict: ...

    async def restore_cell_embedding(
        self,
        *,
        tenant_id: str,
        cell_id: str,
        vector: list[float],
    ) -> None:
        """Restore an archived vector for an existing canonical BrainCell."""
        ...

    async def fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict: ...

    async def terminal_fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict: ...

    async def get_embedding_status(
        self, *, tenant_id: str, cell_id: str, content_hash: str | None, profile: str
    ) -> JsonDict: ...

    async def get_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str
    ) -> JsonDict | None: ...

    async def mark_embedding_skipped(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict: ...
