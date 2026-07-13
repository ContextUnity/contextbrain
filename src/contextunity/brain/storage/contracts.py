"""Canonical storage protocol for Brain backends.

Defines the handler-facing contract that both PostgresBrainStore and
SqliteBrainStore must satisfy. Derived from actual ``self.storage.*``
calls in ``services/brain/src/contextunity/brain/service/handlers/``.
"""

from __future__ import annotations

import sqlite3
from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from contextunity.core.types import JsonDict
from psycopg import AsyncConnection

from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION
from contextunity.brain.storage.postgres.models import (
    GraphEdge,
    GraphNode,
    GraphTraversalResult,
    ScopePath,
    SearchResult,
)

# The only two backends implementing this Protocol: Postgres yields a
# psycopg async connection, SQLite yields a stdlib sqlite3 connection. No
# caller inspects the yielded object generically through the Protocol-typed
# `storage` reference (grep-verified) — the union is the honest type, not a
# stand-in for "don't know".
TenantConnection = AsyncConnection[object] | sqlite3.Connection


class _BrainCellStorageProtocol(Protocol):
    """Canonical BrainCell persistence surface shared by service and admin stores."""

    async def upsert_cell(
        self,
        *,
        tenant_id: str,
        cell_kind: str,
        content: str,
        metadata: JsonDict | None = None,
        cell_id: str | None = None,
        user_id: str | None = None,
        scope_path: str | None = None,
        content_hash: str | None = None,
        source_type: str = "manual",
        source_ref: str | None = None,
        confidence: float = 0.5,
        visibility: str = "tenant",
    ) -> JsonDict:
        """Upsert a canonical BrainCell (idempotent on content_hash when supplied)."""
        ...

    async def query_cells(
        self,
        *,
        tenant_id: str,
        query_text: str | None = None,
        cell_kind: str | None = None,
        source_type: str | None = None,
        scope_path: str | None = None,
        metadata_filter: JsonDict | None = None,
        limit: int = 10,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[JsonDict]:
        """Query BrainCells with optional filters."""
        ...

    async def get_cell(
        self, *, tenant_id: str, cell_id: str, user_id: str | None = None
    ) -> JsonDict | None:
        """Retrieve one tenant-owned BrainCell by ID."""
        ...


@runtime_checkable
class BrainStorageProtocol(_BrainCellStorageProtocol, Protocol):
    """Minimum storage interface consumed by Brain gRPC handlers.

    Every method listed here is called by at least one handler mixin.
    Backends that do not support an operation must raise a typed error
    (e.g. ``UnsupportedLocalModeError``), never return silent empty success.
    """

    # ── Schema lifecycle ──────────────────────────────────────────

    async def ensure_schema(
        self,
        *,
        include_commerce: bool = False,
        vector_dim: int = DEFAULT_EMBEDDING_DIMENSION,
    ) -> None:
        """Create or migration-verify the database tables and indices.

        Args:
            include_commerce: Whether to provision e-commerce specific schemas
                (product indices, matcher tables).
            vector_dim: Dimension of vector embedding columns. Defaults to the
                deployment-wide embedding dimension.
        """
        ...

    def vector_backend_available(self) -> bool:
        """Return whether this backend can execute vector similarity operations."""
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
        scope: ScopePath | None = None,
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

    # ── BrainSynapse ────────────────────────────────────────────────
    # Handler: SynapseHandlersMixin

    async def record_synapse(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        action_type: str,
        action_data: JsonDict | None = None,
        action_data_ref: str | None = None,
        thought_trace_ref: str | None = None,
        content_hash: str | None = None,
        graph_name: str | None = None,
        graph_run_id: str | None = None,
        node_id: str | None = None,
        node_name: str | None = None,
        node_role: str = "worker",
        scope_path: str | None = None,
        context_summary: str | None = None,
        client_id: str | None = None,
        fault_class: str | None = None,
        status: str = "active",
        q_action: float = 0.5,
        q_hypothesis: float = 0.5,
        q_relevance: float = 0.5,
        metadata: JsonDict | None = None,
    ) -> JsonDict:
        """Record one BrainSynapse learning trace.

        Args:
            tenant_id: Tenant partition ID.
            agent_id: Agent/actor/node owner.
            action_type: e.g. ``"plan"``, ``"tool_call"``, ``"llm_prompt"``, ``"route"``.
            action_data: Small structured action payload, stored inline.
            action_data_ref: PassByRef pointer, used instead of ``action_data`` for large payloads.
            thought_trace_ref: Optional reasoning/provenance PassByRef pointer.
            content_hash: Content hash of the referenced data, when a ref is used.
            graph_name: Graph/manifest name.
            graph_run_id: Run identifier shared by every Synapse from one execution.
            node_id: Graph node identifier that produced the trace.
            node_name: Human-readable node name.
            node_role: One of ``"planner"``, ``"worker"``, ``"terminal"``, ``"router"``.
            scope_path: Memory scope path (ltree) for later graph-first retrieval.
            context_summary: Short summary of the context that led to the action.
            client_id: Optional originating client identifier.
            fault_class: One of ``"agent_fault"``, ``"infra_fault"``, ``"upstream_fault"``,
                ``"policy_fault"``, ``"reference_fault"``, or ``None``.
            status: Lifecycle status, defaults to ``"active"``.
            q_action: Action-quality Q-value, clamped to ``[0.0, 1.0]``.
            q_hypothesis: Reasoning/plan-quality Q-value, clamped to ``[0.0, 1.0]``.
            q_relevance: Context/retrieval-relevance Q-value, clamped to ``[0.0, 1.0]``.
            metadata: Extensible metadata (phase, source, provenance, fixture markers, etc.).

        Returns:
            A dict with at least ``{id, q_action, q_hypothesis, q_relevance, q_composite, created_at}``.
        """
        ...

    async def query_synapses(
        self,
        *,
        tenant_id: str,
        action_type: str | None = None,
        agent_id: str | None = None,
        node_role: str | None = None,
        status: str | None = None,
        scope_path: str | None = None,
        min_q: float = 0.6,
        limit: int = 5,
    ) -> list[JsonDict]:
        """Query BrainSynapses, ranked by ``q_composite`` and bounded by ``limit``.

        Args:
            tenant_id: Tenant partition ID to search.
            action_type: Optional action-type filter.
            agent_id: Optional agent filter.
            node_role: Optional node-role filter.
            status: Optional exact lifecycle-status filter. When omitted, defaults to
                ``status IN ('active', 'confirmed')`` — the production-learning set.
            scope_path: Optional ltree scope filter (matches the path and its descendants).
            min_q: Minimum ``q_composite`` to include.
            limit: Maximum rows to return; always bounded, never a full scan.

        Returns:
            Rows ordered by ``q_composite DESC, updated_at DESC``.
        """
        ...

    async def update_synapse_q(
        self,
        *,
        tenant_id: str,
        synapse_id: str,
        q_action: float | None = None,
        q_hypothesis: float | None = None,
        q_relevance: float | None = None,
        fault_class: str | None = None,
        status: str | None = None,
        metadata_patch: JsonDict | None = None,
        idempotency_key: str | None = None,
    ) -> JsonDict | None:
        """Update Q-values/fault/status on one tenant-owned Synapse.

        Unset fields are left unchanged. ``metadata_patch`` is shallow-merged
        into the existing ``metadata`` (never replaces it wholesale).

        Args:
            tenant_id: Tenant partition ID; the row must belong to this tenant.
            synapse_id: Target Synapse ID.
            q_action: New action-quality Q-value, clamped to ``[0.0, 1.0]``, or ``None`` to leave unchanged.
            q_hypothesis: New reasoning-quality Q-value, or ``None`` to leave unchanged.
            q_relevance: New relevance Q-value, or ``None`` to leave unchanged.
            fault_class: New fault classification, or ``None`` to leave unchanged.
            status: New lifecycle status, or ``None`` to leave unchanged.
            metadata_patch: Metadata keys to merge into existing metadata.
            idempotency_key: A ``review_id`` or ``event_id``. When given and
                already recorded in ``metadata.processed_reward_events``, the
                call is a no-op that returns the current unchanged values
                instead of re-applying — guards against duplicate reward
                delivery on replay.

        Returns:
            The updated row (``{id, q_action, q_hypothesis, q_relevance, q_composite, updated_at}``),
            or ``None`` if no row with that ID exists for this tenant.
        """
        ...

    async def decay_synapses(self, *, tenant_id: str, factor: float = 0.99) -> int:
        """Phase 3/5-ready Q-decay hook — not implemented before Consolidation Cycle lands.

        Feature-flagged off by default (``brain.yml: synapses.decay_enabled``).
        Calling this while the flag is disabled is a caller error, not a
        silent no-op — see ``SynapseDecayDisabledError``.

        Args:
            tenant_id: Tenant partition ID to decay.
            factor: Multiplicative decay factor applied to Q-values.

        Returns:
            Count of rows decayed.
        """
        ...

    async def tenant_connection(
        self, tenant_id: str, user_id: str | None = None
    ) -> AbstractAsyncContextManager[TenantConnection]:
        """Return an async context manager yielding a pool connection with RLS tenant context.

        Usage::

            async with await self.storage.tenant_connection("*", user_id="*") as conn:
                rows = await conn.execute(sql, params)
        """
        ...


@runtime_checkable
class AdminQueryProtocol(_BrainCellStorageProtocol, Protocol):
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

    async def get_cells(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]: ...

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict: ...

    async def get_analytics_summary(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...

    async def get_system_analytics(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...


__all__ = [
    "AdminQueryProtocol",
    "BrainStorageProtocol",
    "TenantConnection",
]
