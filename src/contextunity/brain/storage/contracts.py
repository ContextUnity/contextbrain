"""Canonical storage protocol for Brain backends.

Defines the handler-facing contract that both PostgresKnowledgeStore and
SqliteVecStorageBackend must satisfy. Derived from actual ``self.storage.*``
calls in ``services/brain/src/contextunity/brain/service/handlers/``.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, runtime_checkable

from contextunity.core.types import JsonDict, JsonValue

from contextunity.brain.storage.postgres.models import (
    GraphEdge,
    GraphNode,
    GraphTraversalResult,
    SearchResult,
    TaxonomyPath,
)


@runtime_checkable
class KnowledgeStoreProtocol(Protocol):
    """Minimum storage interface consumed by Brain gRPC handlers.

    Every method listed here is called by at least one handler mixin.
    Backends that do not support an operation must raise a typed error
    (e.g. ``UnsupportedLocalModeError``), never return silent empty success.
    """

    # ── Schema lifecycle ──────────────────────────────────────────

    async def ensure_schema(
        self, *, include_commerce: bool = False, vector_dim: int = 1536
    ) -> None:
        """Create or migration-verify the database tables and indices.

        Args:
            include_commerce: Whether to provision e-commerce specific schemas
                (product indices, matcher tables).
            vector_dim: Dimension of vector embedding columns. Defaults to 1536.
        """
        ...

    # ── Blackboard (Flat Memory) ──────────────────────────────────
    # Handler: BlackboardHandlersMixin

    async def write_blackboard(
        self,
        *,
        tenant_id: str,
        scope_path: str,
        content: JsonDict,
        metadata: JsonDict | None = None,
        ttl_seconds: int | None = None,
        created_by: str | None = None,
    ) -> JsonDict:
        """Write a new entry or update an existing scoped entry in the blackboard.

        The blackboard is a flat memory buffer useful for transient key-value storage
        or context sharing within and across execution stages.

        Args:
            tenant_id: The tenant partition ID.
            scope_path: Scoped namespace path (e.g. "agents/workspace/1").
            content: The payload data to store.
            metadata: Optional indexing or tracking metadata.
            ttl_seconds: Optional expiration time-to-live in seconds.
            created_by: ID of the agent/user creating this entry.

        Returns:
            The created or updated blackboard record dict.
        """
        ...

    async def read_blackboard(
        self,
        *,
        ids: list[str],
        tenant_id: str,
    ) -> list[JsonDict]:
        """Retrieve blackboard records by their unique identifiers.

        Args:
            ids: List of record IDs or scoped paths to fetch.
            tenant_id: The tenant partition ID to enforce isolation.

        Returns:
            A list of retrieved blackboard record dictionaries.
        """
        ...

    async def prune_expired_blackboard(self, *, tenant_id: str | None = None) -> int:
        """Remove blackboard entries whose TTL has passed.

        Args:
            tenant_id: Optional tenant ID filter. If None, prunes expired entries
                across all tenants.

        Returns:
            The number of records pruned from the blackboard.
        """
        ...

    # ── Traces ────────────────────────────────────────────────────
    # Handler: TraceHandlersMixin

    async def log_trace(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        session_id: str | None = None,
        user_id: str | None = None,
        graph_name: str | None = None,
        tool_calls: list[JsonDict] | None = None,
        token_usage: JsonDict | None = None,
        timing_ms: int | None = None,
        security_flags: JsonDict | None = None,
        metadata: JsonDict | None = None,
        provenance: list[str] | None = None,
    ) -> str:
        """Log an execution trace for observability, audit, or billing.

        Captures execution path details, tool usage, model latency, and token metrics.

        Args:
            tenant_id: The tenant partition ID.
            agent_id: Identifier of the invoking agent.
            session_id: Optional session run ID linking multiple traces.
            user_id: Optional ID of the end-user triggering the request.
            graph_name: Name of the active LangGraph/state machine.
            tool_calls: List of structured tool calls performed.
            token_usage: Model-specific token usage dictionary.
            timing_ms: Total operation execution time in milliseconds.
            security_flags: Shield compliance flags or sanitization markers.
            metadata: General execution context metadata.
            provenance: Data source lineage or document references.

        Returns:
            The generated unique trace identifier.
        """
        ...

    async def get_traces(
        self,
        *,
        tenant_id: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        limit: int = 20,
        since: str | None = None,
    ) -> list[JsonDict]:
        """Retrieve historical execution traces matching the specified filters.

        Args:
            tenant_id: The tenant partition ID to search.
            user_id: Optional user ID filter.
            agent_id: Optional agent ID filter.
            session_id: Optional session ID filter.
            limit: Maximum number of traces to return. Defaults to 20.
            since: ISO timestamp string to filter traces created after a point in time.

        Returns:
            A list of trace dictionaries.
        """
        ...

    # ── Taxonomy ──────────────────────────────────────────────────
    # Handler: TaxonomyHandlersMixin

    async def upsert_taxonomy(
        self,
        *,
        tenant_id: str,
        domain: str,
        name: str,
        path: str,
        keywords: list[str],
        metadata: JsonDict | None = None,
    ) -> None:
        """Insert or update a taxonomy node in the hierarchical taxonomy tree.

        Enables structural categorization of concepts, e.g. for PIM catalogs.

        Args:
            tenant_id: The tenant partition ID.
            domain: Taxonomy domain namespace (e.g., "products", "medical").
            name: Readable name of the category/node.
            path: Hierarchical path string (e.g., "hardware/tools/screws").
            keywords: List of search keywords associated with the node.
            metadata: Optional supplementary data dictionary.
        """
        ...

    async def get_all_taxonomy(
        self, *, tenant_id: str, domain: str | None = None
    ) -> list[JsonDict]:
        """List all registered taxonomy categories.

        Args:
            tenant_id: The tenant partition ID.
            domain: Optional domain filter.

        Returns:
            A list of taxonomy node dictionaries.
        """
        ...

    # ── Knowledge Graph ───────────────────────────────────────────
    # Handler: KnowledgeHandlersMixin

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
        scope: TaxonomyPath | None = None,
        source_types: list[str] | None = None,
        user_id: str | None = None,
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

    # ── Episodic Memory ───────────────────────────────────────────
    # Handler: MemoryHandlersMixin

    async def add_episode(
        self,
        *,
        id: str,
        user_id: str,
        content: str,
        tenant_id: str,
        embedding: list[float] | None = None,
        metadata: JsonDict | None = None,
        session_id: str | None = None,
    ) -> None:
        """Store an episodic memory entry representing an interaction segment.

        Stores text content, temporal metadata, and the associated dense vector.

        Args:
            id: Unique identifier for the memory segment.
            user_id: The end-user whom the episode relates to.
            content: The text content of the dialogue or action.
            tenant_id: Tenant owner.
            embedding: Optional dense representation of the text content.
            metadata: Optional metadata (e.g. emotions, entities mentioned).
            session_id: Optional chat/run session identifier.
        """
        ...

    async def get_recent_episodes(
        self, *, user_id: str, tenant_id: str, limit: int = 5
    ) -> list[JsonDict]:
        """Retrieve recent episodic memory records sorted chronologically.

        Args:
            user_id: Filter for the target user.
            tenant_id: Tenant partition ID.
            limit: Max memory segments to retrieve. Defaults to 5.

        Returns:
            A list of episode record dictionaries.
        """
        ...

    async def count_episodes(self, *, tenant_id: str) -> JsonDict:
        """Count the total number of stored episodes within a tenant.

        Args:
            tenant_id: The tenant partition ID.

        Returns:
            A dictionary containing count metrics (e.g., {"total": 1240}).
        """
        ...

    async def get_old_episodes(
        self,
        *,
        tenant_id: str,
        older_than_days: int = 30,
        limit: int = 100,
    ) -> list[JsonDict]:
        """Retrieve old episodes that are candidates for archival or cleanup.

        Args:
            tenant_id: Tenant partition ID.
            older_than_days: Return episodes older than this threshold in days. Defaults to 30.
            limit: Max number of candidate episodes to fetch. Defaults to 100.

        Returns:
            A list of old episode dictionaries.
        """
        ...

    async def delete_old_episodes(
        self,
        *,
        tenant_id: str,
        older_than_days: int = 30,
        episode_ids: list[str] | None = None,
    ) -> int:
        """Purge/delete old episodic memories.

        Args:
            tenant_id: Tenant partition ID.
            older_than_days: Only delete records older than this threshold. Defaults to 30.
            episode_ids: Optional explicit list of IDs to target, overriding older_than_days.

        Returns:
            The number of episodes deleted.
        """
        ...

    # ── User Facts ────────────────────────────────────────────────
    # Handler: MemoryHandlersMixin

    async def upsert_fact(
        self,
        *,
        user_id: str,
        tenant_id: str,
        key: str,
        value: JsonValue,
        confidence: float = 1.0,
        source_id: str | None = None,
    ) -> None:
        """Add or update a distilled fact about a user's preferences or traits.

        Used to persist long-term user personalization parameters.

        Args:
            user_id: The target user.
            tenant_id: Tenant partition ID.
            key: Fact category/name key (e.g., "favorite_sport").
            value: The fact content.
            confidence: Reliability score (0.0 to 1.0). Defaults to 1.0.
            source_id: Optional trace/episode ID from which the fact was extracted.
        """
        ...

    async def get_user_facts(self, *, user_id: str, tenant_id: str) -> list[JsonDict]:
        """Retrieve all long-term personalization facts stored for a user.

        Args:
            user_id: Target user identifier.
            tenant_id: Tenant partition ID.

        Returns:
            A list of user fact dictionaries.
        """
        ...

    async def tenant_connection(
        self, tenant_id: str, user_id: str | None = None
    ) -> AbstractAsyncContextManager[Any]:
        """Return an async context manager yielding a pool connection with RLS tenant context.

        Usage::

            async with await self.storage.tenant_connection("*", user_id="*") as conn:
                rows = await conn.execute(sql, params)
        """
        ...


@runtime_checkable
class AdminQueryProtocol(Protocol):
    """Cross-tenant admin observability queries (Brain Admin RPC backing store).

    All methods are async so handlers can ``await`` uniformly. SQLite backends
    wrap sync SQL bodies; Postgres backends run async queries via tenant_connection.
    """

    async def list_tenants(self) -> list[JsonDict]: ...

    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]: ...

    async def get_trace_details(self, trace_id: str) -> JsonDict | None: ...

    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict: ...

    async def get_session_traces(
        self, *, session_id: str, tenant_id: str | None
    ) -> list[JsonDict]: ...

    async def get_related_episodes(self, trace_id: str) -> list[JsonDict]: ...

    async def get_trace_tenant(self, trace_id: str) -> str | None: ...

    async def search_episodes(
        self,
        *,
        tenant_id: str | None,
        user_id: str | None,
        session_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]: ...

    async def get_knowledge_nodes(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]: ...

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict: ...

    async def get_analytics_summary(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...

    async def get_system_analytics(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...


__all__ = ["AdminQueryProtocol", "KnowledgeStoreProtocol"]
