"""Reward constants for Experience Memory Q-value updates.

All deltas are named constants — never hardcoded in business logic.
Defaults are provisional — calibrate after Phase B pilot with real traffic.

Update formula: q_new = clamp(q_old + delta * LEARNING_RATE, 0.0, 1.0)
Initial Q-values: 0.5 (neutral). Range: [0.0, 1.0].
"""

from __future__ import annotations

# ── Node execution (Local scope) ────────────────────────────────────

REWARD_NODE_SUCCESS: float = 0.05  # node completed without error
PENALTY_AGENT_FAULT: float = -0.3  # node failed due to agent logic
# No constant for infra/upstream — Q unchanged

# ── Session outcome (Global scope, before positional discount) ──────

REWARD_SESSION_CONTINUED: float = 0.03  # user continued conversation
PENALTY_SESSION_ABANDONED: float = -0.05  # user abandoned or rejected
DISCOUNT_FACTOR: float = 0.7  # per-hop decay from terminal

# ── Planner attribution ─────────────────────────────────────────────

REWARD_PATH_SUCCESS: float = 0.05  # chosen path led to success
PENALTY_PATH_FAILURE: float = -0.15  # chosen path led to agent_fault

# ── Explicit review (View dashboard) ────────────────────────────────
# OVERRIDE semantics, not delta. Admin verdict is the highest-authority
# signal. It SETS the Q-value to a fixed level, overriding any
# accumulated automatic signals.

REVIEW_VERIFIED_SET_Q: float = 0.9  # admin confirmed → q forced to 0.9
REVIEW_REJECTED_SET_Q: float = 0.1  # admin rejected  → q forced to 0.1

# ── Pipeline outcome (terminal node only) ───────────────────────────

REWARD_OUTCOME_POSITIVE: float = 0.1  # external business signal confirms success
PENALTY_OUTCOME_NEGATIVE: float = -0.1  # external signal indicates failure

# ── Global ──────────────────────────────────────────────────────────

LEARNING_RATE: float = 0.1  # applied to all deltas

# ── Lifecycle weights (applied during QueryExperiences scoring) ─────
# Multiplied with q_composite to weight experiences by lifecycle state.

LIFECYCLE_WEIGHTS: dict[str, float] = {
    "confirmed": 1.2,
    "active": 1.0,
    "outdated": 0.5,
    "archived": 0.3,
    "contradicted": 0.0,
    "superseded": 0.0,
    "merged": 0.0,
    "deleted": 0.0,
}


def clamp_q(value: float) -> float:
    """Clamp Q-value to valid [0.0, 1.0] range."""
    return max(0.0, min(1.0, value))


def apply_delta(q_old: float, delta: float) -> float:
    """Apply a reward/penalty delta with learning rate, clamped to [0, 1]."""
    return clamp_q(q_old + delta * LEARNING_RATE)


__all__ = [
    "REWARD_NODE_SUCCESS",
    "PENALTY_AGENT_FAULT",
    "REWARD_SESSION_CONTINUED",
    "PENALTY_SESSION_ABANDONED",
    "DISCOUNT_FACTOR",
    "REWARD_PATH_SUCCESS",
    "PENALTY_PATH_FAILURE",
    "REVIEW_VERIFIED_SET_Q",
    "REVIEW_REJECTED_SET_Q",
    "REWARD_OUTCOME_POSITIVE",
    "PENALTY_OUTCOME_NEGATIVE",
    "LEARNING_RATE",
    "LIFECYCLE_WEIGHTS",
    "clamp_q",
    "apply_delta",
]
