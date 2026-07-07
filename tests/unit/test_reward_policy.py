"""Tests for reward-signal application policy.

Covers role-scoped delta application, clamping, idempotency, and training
exclusion for `_test`/`_doc` tenants.
"""

from __future__ import annotations

from contextunity.brain.reward_constants import (
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
from contextunity.brain.reward_policy import (
    PROCESSED_REWARD_EVENTS_KEY,
    REWARD_SOURCES,
    apply_node_execution_reward,
    apply_pipeline_outcome_reward,
    apply_session_outcome_reward,
    explicit_review_absolute_q,
    is_already_processed,
    is_trainable_tenant,
    mark_processed,
    node_execution_delta,
    pipeline_outcome_delta,
    q_targets_for_role,
    session_outcome_delta,
)

NEUTRAL_Q = {"q_action": 0.5, "q_hypothesis": 0.5, "q_relevance": 0.5}


class TestRewardSources:
    def test_exactly_four_sources(self):
        assert REWARD_SOURCES == (
            "node_execution",
            "session_outcome",
            "explicit_review",
            "pipeline_outcome",
        )


class TestTrainableTenant:
    def test_test_and_doc_excluded(self):
        assert is_trainable_tenant("_test") is False
        assert is_trainable_tenant("_doc") is False

    def test_ordinary_tenant_trainable(self):
        assert is_trainable_tenant("acme_backend") is True
        assert is_trainable_tenant("contextmed") is True


class TestQTargetsForRole:
    def test_worker_targets_q_action(self):
        assert q_targets_for_role("worker") == ("q_action",)

    def test_planner_and_router_target_q_hypothesis(self):
        assert q_targets_for_role("planner") == ("q_hypothesis",)
        assert q_targets_for_role("router") == ("q_hypothesis",)

    def test_terminal_targets_q_action_by_default(self):
        assert q_targets_for_role("terminal") == ("q_action",)

    def test_terminal_optionally_includes_relevance(self):
        assert q_targets_for_role("terminal", include_terminal_relevance=True) == (
            "q_action",
            "q_relevance",
        )

    def test_non_terminal_role_ignores_relevance_flag(self):
        assert q_targets_for_role("worker", include_terminal_relevance=True) == ("q_action",)


class TestNodeExecutionReward:
    def test_success_delta_matches_constant(self):
        assert node_execution_delta(success=True) == REWARD_NODE_SUCCESS

    def test_failure_delta_matches_constant(self):
        assert node_execution_delta(success=False) == PENALTY_AGENT_FAULT

    def test_worker_success_updates_only_q_action(self):
        result = apply_node_execution_reward(node_role="worker", current_q=NEUTRAL_Q, success=True)
        assert set(result) == {"q_action"}
        assert result["q_action"] == apply_delta(0.5, REWARD_NODE_SUCCESS)

    def test_planner_failure_updates_only_q_hypothesis(self):
        """Done criteria: planner failure updates q_hypothesis, not every downstream worker."""
        result = apply_node_execution_reward(
            node_role="planner", current_q=NEUTRAL_Q, success=False
        )
        assert set(result) == {"q_hypothesis"}
        assert result["q_hypothesis"] == apply_delta(0.5, PENALTY_AGENT_FAULT)

    def test_worker_tool_failure_updates_that_worker_only(self):
        """Done criteria: worker tool failure updates that worker's Synapse only —
        proven at the function-call level (no other Synapse's current_q is touched)."""
        result = apply_node_execution_reward(
            node_role="worker", current_q={"q_action": 0.7}, success=False
        )
        assert result == {"q_action": apply_delta(0.7, PENALTY_AGENT_FAULT)}


class TestSessionOutcomeReward:
    def test_planner_uses_path_attribution_magnitudes(self):
        delta = session_outcome_delta(node_role="planner", success=True, distance_from_terminal=0)
        assert delta == REWARD_PATH_SUCCESS

    def test_planner_failure_uses_path_failure_magnitude(self):
        delta = session_outcome_delta(node_role="planner", success=False, distance_from_terminal=0)
        assert delta == PENALTY_PATH_FAILURE

    def test_non_planner_uses_generic_session_magnitudes(self):
        delta = session_outcome_delta(node_role="worker", success=True, distance_from_terminal=0)
        assert delta == REWARD_SESSION_CONTINUED
        delta_fail = session_outcome_delta(
            node_role="worker", success=False, distance_from_terminal=0
        )
        assert delta_fail == PENALTY_SESSION_ABANDONED

    def test_positional_discount_applied(self):
        delta = session_outcome_delta(node_role="worker", success=True, distance_from_terminal=2)
        assert delta == REWARD_SESSION_CONTINUED * (DISCOUNT_FACTOR**2)

    def test_negative_distance_treated_as_zero(self):
        delta = session_outcome_delta(node_role="worker", success=True, distance_from_terminal=-3)
        assert delta == REWARD_SESSION_CONTINUED

    def test_apply_session_outcome_reward_updates_role_scoped_field(self):
        result = apply_session_outcome_reward(
            node_role="planner", current_q=NEUTRAL_Q, success=True
        )
        assert set(result) == {"q_hypothesis"}


class TestPipelineOutcomeReward:
    def test_success_delta_matches_constant(self):
        assert pipeline_outcome_delta(success=True) == REWARD_OUTCOME_POSITIVE

    def test_failure_delta_matches_constant(self):
        assert pipeline_outcome_delta(success=False) == PENALTY_OUTCOME_NEGATIVE

    def test_terminal_only_updates_q_action_by_default(self):
        result = apply_pipeline_outcome_reward(current_q=NEUTRAL_Q, success=True)
        assert set(result) == {"q_action"}

    def test_terminal_can_include_relevance(self):
        result = apply_pipeline_outcome_reward(
            current_q=NEUTRAL_Q, success=True, include_relevance=True
        )
        assert set(result) == {"q_action", "q_relevance"}


class TestExplicitReviewIsAbsoluteNotDelta:
    def test_verified_sets_absolute_value(self):
        assert explicit_review_absolute_q("verified") == REVIEW_VERIFIED_SET_Q

    def test_rejected_sets_absolute_value(self):
        assert explicit_review_absolute_q("rejected") == REVIEW_REJECTED_SET_Q


class TestQClampingAtBoundaries:
    """Done criteria: Q below 0 / above 1 clamps."""

    def test_repeated_positive_rewards_clamp_at_one(self):
        q = 0.99
        for _ in range(20):
            q = apply_delta(q, REWARD_NODE_SUCCESS)
        assert 0.0 <= q <= 1.0
        assert q == 1.0

    def test_repeated_negative_rewards_clamp_at_zero(self):
        q = 0.01
        for _ in range(20):
            q = apply_delta(q, PENALTY_AGENT_FAULT)
        assert 0.0 <= q <= 1.0
        assert q == 0.0


class TestIdempotencyHelpers:
    def test_not_processed_when_no_key_given(self):
        assert is_already_processed({}, None) is False

    def test_not_processed_when_metadata_empty(self):
        assert is_already_processed({}, "event-1") is False

    def test_processed_after_marking(self):
        metadata = mark_processed({}, "event-1")
        assert is_already_processed(metadata, "event-1") is True

    def test_marking_is_idempotent_itself(self):
        metadata = mark_processed({}, "event-1")
        metadata = mark_processed(metadata, "event-1")
        assert metadata[PROCESSED_REWARD_EVENTS_KEY] == ["event-1"]

    def test_different_keys_accumulate(self):
        metadata = mark_processed({}, "event-1")
        metadata = mark_processed(metadata, "event-2")
        assert metadata[PROCESSED_REWARD_EVENTS_KEY] == ["event-1", "event-2"]
        assert is_already_processed(metadata, "event-1") is True
        assert is_already_processed(metadata, "event-2") is True
        assert is_already_processed(metadata, "event-3") is False

    def test_marking_with_no_key_is_a_noop(self):
        metadata = mark_processed({"existing": "value"}, None)
        assert metadata == {"existing": "value"}

    def test_mark_processed_preserves_other_metadata_keys(self):
        metadata = mark_processed({"phase": 2, "source": "test"}, "event-1")
        assert metadata["phase"] == 2
        assert metadata["source"] == "test"
        assert metadata[PROCESSED_REWARD_EVENTS_KEY] == ["event-1"]
