"""Payloads for canonical BrainSynapse operations."""

from contextunity.core.faults import AGENT_FAULT, FAULT_CLASSES
from contextunity.core.passbyref import DEFAULT_PASSBYREF_THRESHOLD_BYTES, payload_size_bytes
from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.types import JsonDict
from pydantic import Field, model_validator

from ..core.exceptions import SynapseTenantMismatchError

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
