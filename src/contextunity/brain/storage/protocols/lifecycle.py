"""Schema, blackboard, and trace storage protocols."""

from __future__ import annotations

from typing import Protocol

from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ProtectedExecutionTraceArtifactEnvelope,
)
from contextunity.core.types import JsonDict

from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION


class LifecycleStorageProtocol(Protocol):
    # ── Schema lifecycle ──────────────────────────────────────────

    async def ensure_schema(
        self,
        *,
        vector_dim: int = DEFAULT_EMBEDDING_DIMENSION,
    ) -> None:
        """Create or migration-verify the database tables and indices.

        Args:
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

    async def finalize_execution_trace(self, *, terminal_trace: JsonDict) -> JsonDict:
        """Create or deduplicate one canonical terminal execution trace."""
        ...

    async def reserve_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        lifecycle_profile_id: str,
        request_bytes: int,
    ) -> JsonDict:
        """Create or replay one protected request reservation."""
        ...

    async def finalize_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        expected_revision: int,
        request_bytes: int,
        response_bytes: int,
    ) -> JsonDict:
        """Finalize one protected terminal model-I/O snapshot by CAS."""
        ...

    async def get_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
    ) -> JsonDict | None:
        """Read one exact tenant/project-scoped protected artifact row."""
        ...

    async def begin_archive_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Claim one hot artifact for Worker cold offload by CAS."""
        ...

    async def archive_execution_trace_artifact(
        self,
        *,
        receipt: ExecutionTraceArtifactArchiveReceipt,
        expected_revision: int,
    ) -> JsonDict:
        """Replace hot ciphertext with one verified URI-free archive receipt."""
        ...

    async def begin_restore_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Claim one cold artifact for Worker restore by CAS."""
        ...

    async def stage_restore_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        expected_revision: int,
    ) -> JsonDict:
        """Persist verified ciphertext while retaining the restoring claim."""
        ...

    async def complete_restore_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Commit hot authority only after the cold object was removed."""
        ...

    async def begin_purge_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
        legal_hold: bool,
    ) -> JsonDict:
        """Claim a hot or cold artifact for resumable purge by CAS."""
        ...

    async def purge_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
        legal_hold: bool,
    ) -> JsonDict:
        """Purge ciphertext while preserving a minimal tombstone."""
        ...

    async def delete_old_execution_traces(
        self, *, tenant_id: str, older_than_days: int = 30
    ) -> int:
        """Delete this tenant's execution traces older than the threshold."""
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
