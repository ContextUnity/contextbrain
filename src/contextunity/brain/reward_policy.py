"""Reward-signal application policy for BrainSynapse Q-values.

Turns neutral Synapse Q-value observations into controlled learning signals:
deterministic, clamped, role-scoped updates from four reward sources
(node execution, session outcome, explicit review, pipeline outcome),
without uncontrolled feedback loops. Callers compute the new absolute
Q-value(s) here, then pass them to ``storage.update_synapse_q`` — the wire
contract (``UpdateSynapseQPayload``) takes absolute values, not deltas, so
delta math always resolves to an absolute value before it reaches storage.

No live caller wires this into Router yet — Router does not record Synapse
evidence at all today, so this module is the policy and its proof, ready for
that wiring once a real call site exists.

Caller responsibility: none of the ``apply_*_reward`` functions take or check
``tenant_id`` — they operate purely on the Q-values handed to them. Callers
MUST check ``is_trainable_tenant(tenant_id)`` first and skip calling these
functions entirely for ``_test``/``_doc`` tenants — fixtures and documentation
records must never feed production learning.
"""

from __future__ import annotations

from typing import Literal

from contextunity.core.narrowing import as_str_list, str_list_as_json
from contextunity.core.types import JsonDict

from .reward_constants import (
    DISCOUNT_FACTOR,
    PENALTY_AGENT_FAULT,
    PENALTY_OUTCOME_NEGATIVE,
    PENALTY_PATH_FAILURE,
    PENALTY_SESSION_ABANDONED,
    REVIEW_REJECTED_SET_Q,
    REVIEW_VERIFIED_SET_Q,
    REWARD_NODE_SUCCESS,
    REWARD_OUTCOME_POSITIVE,
    REWARD_PATH_SUCCESS,
    REWARD_SESSION_CONTINUED,
    apply_delta,
)

RewardSource = Literal["node_execution", "session_outcome", "explicit_review", "pipeline_outcome"]

REWARD_SOURCES: tuple[RewardSource, ...] = (
    "node_execution",
    "session_outcome",
    "explicit_review",
    "pipeline_outcome",
)

# `_test` fixtures and `_doc` documentation BrainCells are excluded from
# production learning/export by default.
_TRAINING_EXCLUDED_TENANTS = frozenset({"_test", "_doc"})


def is_trainable_tenant(tenant_id: str) -> bool:
    """Whether Synapses under this tenant may feed automatic reward updates."""
    return tenant_id not in _TRAINING_EXCLUDED_TENANTS


# Role -> the Q dimension a `node_execution` / `session_outcome` /
# `pipeline_outcome` reward primarily touches (scoped attribution: a node's
# role determines which quality judgment its outcome speaks to).
_ROLE_PRIMARY_Q_FIELD: dict[str, str] = {
    "worker": "q_action",
    "planner": "q_hypothesis",
    "router": "q_hypothesis",
    "terminal": "q_action",
}


def q_targets_for_role(
    node_role: str, *, include_terminal_relevance: bool = False
) -> tuple[str, ...]:
    """Which Q dimension(s) an automatic reward update may touch for this role.

    ``terminal`` optionally also updates ``q_relevance`` — off by default,
    since context/retrieval relevance is a separate judgment from
    action/outcome quality and most reward sources have no signal about it.
    """
    primary = _ROLE_PRIMARY_Q_FIELD.get(node_role, "q_action")
    if node_role == "terminal" and include_terminal_relevance:
        return (primary, "q_relevance")
    return (primary,)


def node_execution_delta(*, success: bool) -> float:
    """Reward source 1: per-node execution outcome."""
    return REWARD_NODE_SUCCESS if success else PENALTY_AGENT_FAULT


def session_outcome_delta(
    *, node_role: str, success: bool, distance_from_terminal: int = 0
) -> float:
    """Reward source 2: session-level outcome, positionally discounted.

    A planner node's contribution to session outcome is *which path it
    chose* — attributed with the path-success/failure magnitudes
    (``REWARD_PATH_SUCCESS``/``PENALTY_PATH_FAILURE``), not the generic
    continuation/abandonment magnitudes used for every other role.
    """
    if node_role == "planner":
        base = REWARD_PATH_SUCCESS if success else PENALTY_PATH_FAILURE
    else:
        base = REWARD_SESSION_CONTINUED if success else PENALTY_SESSION_ABANDONED
    discount = DISCOUNT_FACTOR ** max(distance_from_terminal, 0)
    return base * discount


def pipeline_outcome_delta(*, success: bool) -> float:
    """Reward source 4: full-trajectory outcome (terminal node only)."""
    return REWARD_OUTCOME_POSITIVE if success else PENALTY_OUTCOME_NEGATIVE


def explicit_review_absolute_q(verdict: Literal["verified", "rejected"]) -> float:
    """Reward source 3: explicit human/admin review.

    An ABSOLUTE override, never a delta — a reviewer sets the Q-value
    directly rather than nudging it, and the caller must record who/why in
    the Synapse's metadata as provenance.
    """
    return REVIEW_VERIFIED_SET_Q if verdict == "verified" else REVIEW_REJECTED_SET_Q


def apply_node_execution_reward(
    *,
    node_role: str,
    current_q: dict[str, float],
    success: bool,
    include_terminal_relevance: bool = False,
) -> dict[str, float]:
    """Compute the new absolute Q-value(s) for a ``node_execution`` reward.

    Args:
        node_role: One of ``"planner"``, ``"worker"``, ``"terminal"``, ``"router"``.
        current_q: Current ``{"q_action": ..., "q_hypothesis": ..., "q_relevance": ...}``.
        success: Whether the node execution succeeded.
        include_terminal_relevance: See ``q_targets_for_role``.

    Returns:
        Only the Q field(s) that changed, e.g. ``{"q_action": 0.55}`` — pass
        directly as kwargs to ``storage.update_synapse_q``.
    """
    delta = node_execution_delta(success=success)
    targets = q_targets_for_role(node_role, include_terminal_relevance=include_terminal_relevance)
    return {field: apply_delta(current_q[field], delta) for field in targets}


def apply_session_outcome_reward(
    *,
    node_role: str,
    current_q: dict[str, float],
    success: bool,
    distance_from_terminal: int = 0,
    include_terminal_relevance: bool = False,
) -> dict[str, float]:
    """Compute the new absolute Q-value(s) for a ``session_outcome`` reward."""
    delta = session_outcome_delta(
        node_role=node_role, success=success, distance_from_terminal=distance_from_terminal
    )
    targets = q_targets_for_role(node_role, include_terminal_relevance=include_terminal_relevance)
    return {field: apply_delta(current_q[field], delta) for field in targets}


def apply_pipeline_outcome_reward(
    *,
    current_q: dict[str, float],
    success: bool,
    include_relevance: bool = False,
) -> dict[str, float]:
    """Compute the new absolute Q-value(s) for a ``pipeline_outcome`` reward.

    Terminal-node only — the full-trajectory outcome is judged at the end of
    a run, so the caller is responsible for only invoking this for
    terminal-role Synapses.
    """
    delta = pipeline_outcome_delta(success=success)
    targets = q_targets_for_role("terminal", include_terminal_relevance=include_relevance)
    return {field: apply_delta(current_q[field], delta) for field in targets}


# ── Idempotency (explicit review / event replay) ──────────────────────────
#
# A replayed review/event must not double-apply its Q change. A Synapse's own
# `metadata.processed_reward_events` list records every review_id/event_id
# already applied; storage adapters check membership before applying and
# append after — no separate ledger needed.

PROCESSED_REWARD_EVENTS_KEY = "processed_reward_events"


def is_already_processed(metadata: JsonDict, idempotency_key: str | None) -> bool:
    """Whether this review_id/event_id was already applied to this Synapse."""
    if not idempotency_key:
        return False
    return idempotency_key in as_str_list(metadata.get(PROCESSED_REWARD_EVENTS_KEY))


def mark_processed(metadata: JsonDict, idempotency_key: str | None) -> JsonDict:
    """Return a copy of ``metadata`` with ``idempotency_key`` recorded as applied."""
    merged = dict(metadata)
    if not idempotency_key:
        return merged
    ids = as_str_list(metadata.get(PROCESSED_REWARD_EVENTS_KEY))
    if idempotency_key not in ids:
        ids = [*ids, idempotency_key]
    merged[PROCESSED_REWARD_EVENTS_KEY] = str_list_as_json(ids)
    return merged


__all__ = [
    "PROCESSED_REWARD_EVENTS_KEY",
    "REWARD_SOURCES",
    "RewardSource",
    "apply_node_execution_reward",
    "apply_pipeline_outcome_reward",
    "apply_session_outcome_reward",
    "explicit_review_absolute_q",
    "is_already_processed",
    "is_trainable_tenant",
    "mark_processed",
    "node_execution_delta",
    "pipeline_outcome_delta",
    "q_targets_for_role",
    "session_outcome_delta",
]
