"""Pydantic payload models for Brain gRPC operations.

These models provide server-side validation for ContextUnit payloads.
Each RPC method extracts and validates its payload using these models.

Example usage in service:
    from .payloads import SearchPayload

    async def Search(self, request, context):
        unit = ContextUnit.from_protobuf(request)
        params = SearchPayload.model_validate(unit.payload or {})
        # params.tenant_id, params.query_text, etc.
"""

from contextunity.core.faults import AGENT_FAULT, FAULT_CLASSES
from contextunity.core.passbyref import DEFAULT_PASSBYREF_THRESHOLD_BYTES, payload_size_bytes
from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.types import JsonDict, JsonValue, is_object_dict
from pydantic import Field, model_validator

from .core.exceptions import SynapseTenantMismatchError

# =====================================================
# Core Knowledge Operations
# =====================================================


class SearchPayload(StrictPayloadModel):
    """Payload for Search RPC."""

    tenant_id: str
    user_id: str | None = None
    query_text: str
    limit: int = 10
    min_score: float = 0.0
    source_types: list[str] = Field(default_factory=list)


class GraphSearchPayload(StrictPayloadModel):
    """Payload for GraphSearch RPC.

    Structural graph traversal starting from known entity IDs,
    walking edges up to max_hops.
    """

    tenant_id: str
    user_id: str | None = None
    entrypoint_ids: list[str]
    max_hops: int = Field(default=2, ge=1, le=10)
    allowed_relations: list[str] = Field(default_factory=list)
    max_results: int = Field(default=200, ge=1, le=1000)


class CreateKGRelationPayload(StrictPayloadModel):
    """Payload for CreateKGRelation RPC."""

    tenant_id: str
    user_id: str | None = None
    source_type: str
    source_id: str
    relation: str
    target_type: str
    target_id: str


class UpsertPayload(StrictPayloadModel):
    """Payload for Upsert RPC."""

    tenant_id: str
    user_id: str | None = None
    content: str
    source_type: str
    metadata: JsonDict = Field(default_factory=dict)


class QueryMemoryPayload(StrictPayloadModel):
    """Payload for QueryMemory RPC."""

    tenant_id: str
    user_id: str | None = None
    content: str
    filters: JsonDict = Field(default_factory=dict)


# Episodic & Entity Memory
# =====================================================


class AddEpisodePayload(StrictPayloadModel):
    """Payload for AddEpisode RPC."""

    tenant_id: str
    user_id: str | None = None
    session_id: str | None = None
    content: str
    metadata: JsonDict = Field(default_factory=dict)


class UpsertFactPayload(StrictPayloadModel):
    """Payload for UpsertFact RPC."""

    user_id: str
    tenant_id: str
    key: str
    value: JsonValue
    confidence: float = 1.0
    source_id: str | None = None


class GetRecentEpisodesPayload(StrictPayloadModel):
    """Payload for GetRecentEpisodes RPC."""

    tenant_id: str
    user_id: str
    limit: int = 5


class GetUserFactsPayload(StrictPayloadModel):
    """Payload for GetUserFacts RPC."""

    tenant_id: str
    user_id: str


class RetentionCleanupPayload(StrictPayloadModel):
    """Payload for RetentionCleanup RPC."""

    tenant_id: str
    older_than_days: int = 30
    episode_ids: list[str] | None = None


class GetEpisodeStatsPayload(StrictPayloadModel):
    """Payload for GetEpisodeStats RPC."""

    tenant_id: str


# =====================================================
# Agent Traces
# =====================================================


class LogTracePayload(StrictPayloadModel):
    """Payload for LogTrace RPC."""

    tenant_id: str
    agent_id: str
    session_id: str | None = None
    user_id: str | None = None
    graph_name: str | None = None
    tool_calls: list[JsonDict] = Field(default_factory=list)
    token_usage: JsonDict = Field(default_factory=dict)
    timing_ms: int | None = None
    security_flags: JsonDict = Field(default_factory=dict)
    metadata: JsonDict = Field(default_factory=dict)
    provenance: list[str] = Field(default_factory=list)


class GetTracesPayload(StrictPayloadModel):
    """Payload for GetTraces RPC."""

    tenant_id: str
    agent_id: str | None = None
    session_id: str | None = None
    user_id: str | None = None
    limit: int = 20
    since: str | None = None  # ISO datetime


# =====================================================
# Taxonomy Operations
# =====================================================


class UpsertTaxonomyPayload(StrictPayloadModel):
    """Payload for UpsertTaxonomy RPC."""

    tenant_id: str
    domain: str = "general"
    name: str
    path: str | None = None
    keywords: list[str] = Field(default_factory=list)
    metadata: JsonDict = Field(default_factory=dict)


class GetTaxonomyPayload(StrictPayloadModel):
    """Payload for GetTaxonomy RPC."""

    tenant_id: str
    domain: str | None = None


# =====================================================
# Blackboard Operations (Flat Memory)
# =====================================================


class WriteBlackboardPayload(StrictPayloadModel):
    """Payload for WriteBlackboard RPC."""

    tenant_id: str = Field(min_length=1)
    scope_path: str  # LTREE path: 'tenant.project.session.step'
    content: JsonDict
    metadata: JsonDict = Field(default_factory=dict)
    ttl_seconds: int | None = None
    created_by: str | None = None


class ReadBlackboardPayload(StrictPayloadModel):
    """Payload for ReadBlackboard RPC."""

    ids: list[str]  # UUID strings


# =====================================================
# Gardener / Human-in-the-Loop
# =====================================================


class GetPendingPayload(StrictPayloadModel):
    """Payload for GetPendingVerifications RPC."""

    tenant_id: str
    limit: int = 50


class SubmitVerificationPayload(StrictPayloadModel):
    """Payload for SubmitVerification RPC."""

    id: str
    enrichment_json: str


class MatchDuckDBPayload(StrictPayloadModel):
    """Payload for MatchDuckDB RPC (presigned Parquet URLs)."""

    tenant_id: str
    unmatched_url: str
    canonical_url: str
    leftovers_put_url: str

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_url_keys(cls, data: object) -> object:
        """Accept legacy alias keys (``url_unmatched``, etc.) from older clients."""
        if not is_object_dict(data):
            return data
        raw = dict(data)
        if "unmatched_url" not in raw and "url_unmatched" in raw:
            raw["unmatched_url"] = raw["url_unmatched"]
        if "canonical_url" not in raw and "url_canonical" in raw:
            raw["canonical_url"] = raw["url_canonical"]
        if "leftovers_put_url" not in raw and "url_leftovers_put" in raw:
            raw["leftovers_put_url"] = raw["url_leftovers_put"]
        _ = raw.pop("url_unmatched", None)
        _ = raw.pop("url_canonical", None)
        _ = raw.pop("url_leftovers_put", None)
        return raw


# =====================================================
# BrainSynapse (Flat Memory Phase B — canonical learning-trace contract)
# =====================================================
#
# The physical table remains canonical `synapses`; these are the public
# payload names going forward.

_SYNAPSE_NODE_ROLES = ("planner", "worker", "terminal", "router")
_SYNAPSE_STATUSES = (
    "active",
    "confirmed",
    "outdated",
    "archived",
    "contradicted",
    "superseded",
    "merged",
    "deleted",
)
# Single source of truth for the fault taxonomy lives in contextunity.core.faults;
# not re-declared here.
_SYNAPSE_FAULT_CLASSES = FAULT_CLASSES


def _reject_spoofed_tenant(tenant_id: str | None) -> None:
    """Reject a payload ``tenant_id`` that contradicts the caller's token scope.

    ``tenant_id`` is optional on Synapse payloads — new callers should omit it
    and let the token decide. When present, it must match the verified auth
    context's allowed tenants; a mismatch is a ``policy_fault``, not a
    validation error, so it must not be silently coerced.
    """
    if not tenant_id:
        return
    from contextunity.core.authz.context import get_auth_context

    auth_ctx = get_auth_context()
    if auth_ctx is not None and not auth_ctx.can_access_tenant(tenant_id):
        raise SynapseTenantMismatchError(tenant_id=tenant_id)


def _reject_oversized_inline_action_data(
    action_data: JsonDict, action_data_ref: str | None
) -> None:
    """Fail closed on inline ``action_data`` that should have been a PassByRef.

    The payload boundary cannot perform the (async) Blackboard write itself —
    that is the caller/handler's job via ``maybe_pass_by_ref`` — but it can
    refuse to accept an oversized inline blob so large traces never land in
    ``synapses.action_data`` directly.
    """
    if action_data_ref or not action_data:
        return
    if payload_size_bytes(action_data) > DEFAULT_PASSBYREF_THRESHOLD_BYTES:
        raise ValueError(
            f"action_data exceeds the PassByRef threshold ({DEFAULT_PASSBYREF_THRESHOLD_BYTES} bytes); "
            "write it via maybe_pass_by_ref() and pass action_data_ref instead of an inline blob."
        )


_REWARD_SOURCES = ("node_execution",)


def _has_q_update(payload: "UpdateSynapseQPayload") -> bool:
    return (
        payload.q_action is not None
        or payload.q_hypothesis is not None
        or payload.q_relevance is not None
        or payload.reward_source is not None
    )


def _validate_q_update_provenance(payload: "UpdateSynapseQPayload") -> None:
    if not _has_q_update(payload):
        return
    if not payload.review_id and not payload.event_id:
        raise ValueError("Q-value updates require review_id or event_id provenance")
    if payload.review_id:
        return
    if payload.reward_source is not None:
        # The reward_source path computes "no change" for a non-agent
        # fault on its own (see the handler's _apply_reward_source) — unlike
        # a direct q_action/q_hypothesis/q_relevance set, it never needs
        # review_id just because fault_class is non-agent.
        return
    if payload.fault_class is not None and payload.fault_class != AGENT_FAULT:
        raise ValueError("non-agent fault Q changes require review_id explicit review provenance")


def _validate_reward_source(payload: "UpdateSynapseQPayload") -> None:
    if payload.reward_source is None:
        return
    if payload.reward_source not in _REWARD_SOURCES:
        raise ValueError(
            f"reward_source must be one of {_REWARD_SOURCES}, got {payload.reward_source!r}"
        )
    if (
        payload.q_action is not None
        or payload.q_hypothesis is not None
        or payload.q_relevance is not None
    ):
        raise ValueError(
            "reward_source and explicit q_action/q_hypothesis/q_relevance are mutually exclusive"
        )
    if payload.node_role is None:
        raise ValueError("reward_source requires node_role")
    if payload.node_role not in _SYNAPSE_NODE_ROLES:
        raise ValueError(
            f"node_role must be one of {_SYNAPSE_NODE_ROLES}, got {payload.node_role!r}"
        )
    if payload.success is None:
        raise ValueError("reward_source='node_execution' requires success")
    if (
        payload.current_q_action is None
        or payload.current_q_hypothesis is None
        or payload.current_q_relevance is None
    ):
        raise ValueError(
            "reward_source requires current_q_action/current_q_hypothesis/current_q_relevance"
        )


class RecordSynapsePayload(StrictPayloadModel):
    """Payload for RecordSynapse RPC — canonical BrainSynapse write contract."""

    tenant_id: str | None = None
    agent_id: str
    action_type: str
    action_data: JsonDict = Field(default_factory=dict)
    action_data_ref: str | None = None
    thought_trace_ref: str | None = None
    content_hash: str | None = None
    graph_name: str | None = None
    graph_run_id: str | None = None
    node_id: str | None = None
    node_name: str | None = None
    node_role: str = "worker"
    scope_path: str | None = None
    context_summary: str | None = None
    client_id: str | None = None
    fault_class: str | None = None
    status: str = "active"
    q_action: float = Field(default=0.5, ge=0.0, le=1.0)
    q_hypothesis: float = Field(default=0.5, ge=0.0, le=1.0)
    q_relevance: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: JsonDict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_synapse_record(self) -> "RecordSynapsePayload":
        if self.node_role not in _SYNAPSE_NODE_ROLES:
            raise ValueError(
                f"node_role must be one of {_SYNAPSE_NODE_ROLES}, got {self.node_role!r}"
            )
        if self.status not in _SYNAPSE_STATUSES:
            raise ValueError(f"status must be one of {_SYNAPSE_STATUSES}, got {self.status!r}")
        if self.fault_class is not None and self.fault_class not in _SYNAPSE_FAULT_CLASSES:
            raise ValueError(
                f"fault_class must be one of {_SYNAPSE_FAULT_CLASSES}, got {self.fault_class!r}"
            )
        _reject_oversized_inline_action_data(self.action_data, self.action_data_ref)
        _reject_spoofed_tenant(self.tenant_id)
        return self


class QuerySynapsesPayload(StrictPayloadModel):
    """Payload for QuerySynapses RPC — canonical BrainSynapse query contract."""

    tenant_id: str | None = None
    action_type: str | None = None
    agent_id: str | None = None
    node_role: str | None = None
    status: str | None = None
    scope_path: str | None = None
    min_q: float = Field(default=0.6, ge=0.0, le=1.0)
    limit: int = Field(default=5, ge=1, le=50)
    exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    metadata_filter: JsonDict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_synapse_query(self) -> "QuerySynapsesPayload":
        if self.node_role is not None and self.node_role not in _SYNAPSE_NODE_ROLES:
            raise ValueError(
                f"node_role must be one of {_SYNAPSE_NODE_ROLES}, got {self.node_role!r}"
            )
        if self.status is not None and self.status not in _SYNAPSE_STATUSES:
            raise ValueError(f"status must be one of {_SYNAPSE_STATUSES}, got {self.status!r}")
        _reject_spoofed_tenant(self.tenant_id)
        return self


class UpdateSynapseQPayload(StrictPayloadModel):
    """Payload for UpdateSynapseQ RPC — canonical BrainSynapse Q-update contract.

    Either supply absolute ``q_action``/``q_hypothesis``/``q_relevance``
    values directly, or set ``reward_source`` to have the handler compute
    them via ``reward_policy.apply_node_execution_reward`` from the
    caller-supplied ``current_q_*`` baseline — the two modes are mutually
    exclusive.
    """

    synapse_id: str
    q_action: float | None = Field(default=None, ge=0.0, le=1.0)
    q_hypothesis: float | None = Field(default=None, ge=0.0, le=1.0)
    q_relevance: float | None = Field(default=None, ge=0.0, le=1.0)
    fault_class: str | None = None
    status: str | None = None
    review_id: str | None = None
    event_id: str | None = None
    metadata: JsonDict = Field(default_factory=dict)
    reward_source: str | None = None
    node_role: str | None = None
    success: bool | None = None
    current_q_action: float | None = Field(default=None, ge=0.0, le=1.0)
    current_q_hypothesis: float | None = Field(default=None, ge=0.0, le=1.0)
    current_q_relevance: float | None = Field(default=None, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _validate_synapse_update(self) -> "UpdateSynapseQPayload":
        if self.status is not None and self.status not in _SYNAPSE_STATUSES:
            raise ValueError(f"status must be one of {_SYNAPSE_STATUSES}, got {self.status!r}")
        if self.fault_class is not None and self.fault_class not in _SYNAPSE_FAULT_CLASSES:
            raise ValueError(
                f"fault_class must be one of {_SYNAPSE_FAULT_CLASSES}, got {self.fault_class!r}"
            )
        _validate_reward_source(self)
        _validate_q_update_provenance(self)
        return self


# Admin RPCs (WS-8) — require admin:read
# Cross-tenant observability owned by Brain (replaces View brain_db RLS bypass).


class ListTenantsPayload(StrictPayloadModel):
    """Payload for ListTenants admin RPC.

    No fields required — token scoping determines which tenants are returned.
    """


class AdminSearchTracesPayload(StrictPayloadModel):
    """Payload for AdminSearchTraces admin RPC.

    tenant_id is required unless the token has admin:all.
    The service enforces this: empty allowed_tenants is NEVER treated as "all tenants".
    """

    tenant_id: str | None = None
    service: str | None = None
    agent_id: str | None = None
    status: str | None = None
    hours: int | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class AdminGetTraceDetailsPayload(StrictPayloadModel):
    """Payload for AdminGetTraceDetails admin RPC."""

    trace_id: str


class AdminGetSystemAnalyticsPayload(StrictPayloadModel):
    """Payload for AdminGetSystemAnalytics admin RPC.

    tenant_id is required unless the token has admin:all.
    """

    hours: int | None = None
    tenant_id: str | None = None


class AdminGetMemoryLayerStatsPayload(StrictPayloadModel):
    """Payload for AdminGetMemoryLayerStats admin RPC.

    tenant_id is required unless the token has admin:all.
    """

    layer: str | None = None
    tenant_id: str | None = None


class AdminGetFilterOptionsPayload(StrictPayloadModel):
    """Payload for AdminGetFilterOptions admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None


class AdminGetSessionTracesPayload(StrictPayloadModel):
    """Payload for AdminGetSessionTraces admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    session_id: str
    tenant_id: str | None = None


class AdminGetRelatedEpisodesPayload(StrictPayloadModel):
    """Payload for AdminGetRelatedEpisodes admin RPC.

    Tenant scope is resolved from the trace's own tenant_id after fetch.
    """

    trace_id: str


class AdminSearchEpisodesPayload(StrictPayloadModel):
    """Payload for AdminSearchEpisodes admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    hours: int | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class AdminGetCellsPayload(StrictPayloadModel):
    """Payload for AdminGetCells admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None
    kind: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class AdminGetAnalyticsSummaryPayload(StrictPayloadModel):
    """Payload for AdminGetAnalyticsSummary admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None
    hours: int | None = None
