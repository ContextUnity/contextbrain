"""Conversation History and BrainSynapse storage protocols."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from contextunity.core.sdk.conversation import (
    ConversationAppendReceipt,
    ConversationHistoryStats,
    ConversationKind,
    ConversationProjection,
    ConversationRecord,
    ConversationRetentionReceipt,
    ConversationRole,
)
from contextunity.core.types import JsonDict

from contextunity.brain.payloads.outcomes import OutcomeObservationPayload
from contextunity.brain.storage.protocols.connection import TenantConnection


class MemoryStorageProtocol(Protocol):
    # ── Conversation History ──────────────────────────────────────
    # Handler: MemoryHandlersMixin

    async def append_conversation_record(
        self,
        *,
        record_id: UUID,
        tenant_id: str,
        user_id: str,
        session_id: str | None,
        role: ConversationRole,
        kind: ConversationKind,
        content: str,
        content_hash: str,
        source_hash: str,
        graph_run_id: UUID | None,
        metadata_version: int,
        idempotency_key: str,
        metadata: JsonDict,
        created_at: datetime | None = None,
    ) -> ConversationAppendReceipt:
        """Append one immutable record under durable idempotency authority."""
        ...

    async def query_conversation_history(
        self,
        *,
        tenant_id: str,
        projection: ConversationProjection,
        user_id: str | None,
        session_id: str | None,
        graph_run_id: UUID | None,
        older_than_days: int | None,
        limit: int,
        offset: int,
    ) -> list[ConversationRecord]:
        """Return one bounded canonical projection."""
        ...

    async def get_conversation_history_stats(self, *, tenant_id: str) -> ConversationHistoryStats:
        """Return content-free tenant statistics."""
        ...

    async def apply_conversation_retention(
        self,
        *,
        tenant_id: str,
        record_ids: list[UUID],
        cutoff: datetime,
        policy_version: Literal["contextunity.conversation-retention/v1"],
        hold_evidence_hash: str,
    ) -> ConversationRetentionReceipt:
        """Apply explicit evidence-backed owner retention."""
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

    async def import_outcome_observation_record(self, *, tenant_id: str, record: JsonDict) -> None:
        """Restore immutable evidence without replaying its learning effect."""
        ...

    async def resolve_outcome_observation(
        self,
        *,
        tenant_id: str,
        observation: OutcomeObservationPayload,
        policy_version: str,
    ) -> JsonDict:
        """Store one immutable observation and atomically resolve eligible learning."""
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
