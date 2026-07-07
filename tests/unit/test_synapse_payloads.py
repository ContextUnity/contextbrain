"""Canonical BrainSynapse payload tests.

Covers payload defaults, tenant-spoof rejection, and the PassByRef size
guard on inline ``action_data``.
"""

from __future__ import annotations

import pytest
from contextunity.core.authz.context import (
    VerifiedAuthContext,
    reset_auth_context,
    set_auth_context,
)
from contextunity.core.passbyref import DEFAULT_PASSBYREF_THRESHOLD_BYTES
from contextunity.core.tokens import ContextToken
from pydantic import ValidationError

from contextunity.brain.core.exceptions import SynapseTenantMismatchError
from contextunity.brain.payloads import (
    QuerySynapsesPayload,
    RecordSynapsePayload,
    UpdateSynapseQPayload,
)


class TestRecordSynapsePayloadDefaults:
    def test_minimal_payload_defaults(self):
        payload = RecordSynapsePayload(agent_id="agent-1", action_type="tool_call")

        assert payload.q_action == 0.5
        assert payload.q_hypothesis == 0.5
        assert payload.q_relevance == 0.5
        assert payload.status == "active"
        assert payload.node_role == "worker"
        assert payload.tenant_id is None
        assert payload.metadata == {}

    def test_rejects_unknown_node_role(self):
        with pytest.raises(ValidationError, match="node_role"):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", node_role="bogus")

    def test_rejects_unknown_status(self):
        with pytest.raises(ValidationError, match="status"):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", status="bogus")

    def test_rejects_unknown_fault_class(self):
        with pytest.raises(ValidationError, match="fault_class"):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", fault_class="bogus")

    def test_q_values_clamped_by_bounds(self):
        with pytest.raises(ValidationError):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", q_action=1.5)
        with pytest.raises(ValidationError):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", q_action=-0.1)


class TestOversizedActionDataRejected:
    def test_large_inline_action_data_rejected(self):
        big_blob = {"payload": "x" * (DEFAULT_PASSBYREF_THRESHOLD_BYTES + 1)}

        with pytest.raises(ValidationError, match="PassByRef"):
            RecordSynapsePayload(agent_id="agent-1", action_type="x", action_data=big_blob)

    def test_large_action_data_allowed_with_ref(self):
        big_blob = {"payload": "x" * (DEFAULT_PASSBYREF_THRESHOLD_BYTES + 1)}

        payload = RecordSynapsePayload(
            agent_id="agent-1",
            action_type="x",
            action_data=big_blob,
            action_data_ref="blackboard:some-uuid",
        )
        assert payload.action_data_ref == "blackboard:some-uuid"

    def test_small_inline_action_data_allowed(self):
        payload = RecordSynapsePayload(agent_id="agent-1", action_type="x", action_data={"k": "v"})
        assert payload.action_data == {"k": "v"}


@pytest.fixture(autouse=True)
def _clear_auth_context():
    yield
    reset_auth_context()


class TestTenantSpoofRejection:
    def _set_token_scope(self, *allowed_tenants: str) -> None:
        token = ContextToken(
            token_id="t1", permissions=("memory:write",), allowed_tenants=allowed_tenants
        )
        set_auth_context(VerifiedAuthContext.from_token(token, "raw-token"))

    def test_matching_tenant_is_accepted(self):
        self._set_token_scope("acme")
        payload = RecordSynapsePayload(agent_id="agent-1", action_type="x", tenant_id="acme")
        assert payload.tenant_id == "acme"

    def test_mismatched_tenant_rejected_as_policy_fault(self):
        self._set_token_scope("acme")

        with pytest.raises(SynapseTenantMismatchError) as excinfo:
            RecordSynapsePayload(agent_id="agent-1", action_type="x", tenant_id="someone-else")

        assert excinfo.value.fault_class == "policy_fault"
        assert excinfo.value.code == "BRAIN_SYNAPSE_TENANT_MISMATCH"

    def test_omitted_tenant_id_is_always_accepted(self):
        self._set_token_scope("acme")
        payload = RecordSynapsePayload(agent_id="agent-1", action_type="x")
        assert payload.tenant_id is None

    def test_no_auth_context_does_not_reject(self):
        # No interceptor context set (e.g. local/unit-test construction) — nothing to check against.
        payload = RecordSynapsePayload(agent_id="agent-1", action_type="x", tenant_id="anything")
        assert payload.tenant_id == "anything"

    def test_query_payload_also_rejects_spoofed_tenant(self):
        self._set_token_scope("acme")
        with pytest.raises(SynapseTenantMismatchError):
            QuerySynapsesPayload(tenant_id="someone-else")


class TestQuerySynapsesPayloadDefaults:
    def test_defaults(self):
        payload = QuerySynapsesPayload()
        assert payload.min_q == 0.6
        assert payload.limit == 5
        assert payload.exploration_rate == 0.1
        assert payload.status is None

    def test_limit_bounds(self):
        with pytest.raises(ValidationError):
            QuerySynapsesPayload(limit=0)
        with pytest.raises(ValidationError):
            QuerySynapsesPayload(limit=51)

    def test_status_accepts_valid_lifecycle_value(self):
        payload = QuerySynapsesPayload(status="archived")
        assert payload.status == "archived"

    def test_status_rejects_unknown_value(self):
        with pytest.raises(ValidationError, match="status"):
            QuerySynapsesPayload(status="bogus")


class TestUpdateSynapseQPayload:
    def test_requires_synapse_id(self):
        with pytest.raises(ValidationError):
            UpdateSynapseQPayload()

    def test_partial_q_updates_allowed(self):
        payload = UpdateSynapseQPayload(synapse_id="syn-1", q_action=0.9, event_id="evt-1")
        assert payload.q_action == 0.9
        assert payload.q_hypothesis is None

    def test_q_update_requires_provenance_key(self):
        with pytest.raises(ValidationError, match="review_id or event_id"):
            UpdateSynapseQPayload(synapse_id="syn-1", q_action=0.9)

    def test_non_agent_fault_q_update_requires_explicit_review(self):
        with pytest.raises(ValidationError, match="non-agent fault"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                q_action=0.9,
                fault_class="infra_fault",
                event_id="evt-1",
            )

    def test_explicit_review_can_set_non_agent_fault_q(self):
        payload = UpdateSynapseQPayload(
            synapse_id="syn-1",
            q_action=0.9,
            fault_class="infra_fault",
            review_id="review-1",
        )
        assert payload.review_id == "review-1"

    def test_no_legacy_experience_alias(self):
        payload = UpdateSynapseQPayload(synapse_id="syn-1", q_action=0.9, event_id="evt-1")
        legacy_name = "experience" + "_id"
        assert not hasattr(payload, legacy_name)

    def test_reward_source_requires_node_role(self):
        with pytest.raises(ValidationError, match="reward_source requires node_role"):
            UpdateSynapseQPayload(
                synapse_id="syn-1", reward_source="node_execution", success=True, event_id="evt-1"
            )

    def test_reward_source_requires_success(self):
        with pytest.raises(ValidationError, match="requires success"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                reward_source="node_execution",
                node_role="worker",
                event_id="evt-1",
            )

    def test_reward_source_rejects_unknown_value(self):
        with pytest.raises(ValidationError, match="reward_source must be one of"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                reward_source="session_outcome",
                node_role="worker",
                success=True,
                event_id="evt-1",
            )

    def test_reward_source_mutually_exclusive_with_explicit_q(self):
        with pytest.raises(ValidationError, match="mutually exclusive"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                reward_source="node_execution",
                node_role="worker",
                success=True,
                q_action=0.9,
                current_q_action=0.5,
                current_q_hypothesis=0.5,
                current_q_relevance=0.5,
                event_id="evt-1",
            )

    def test_reward_source_still_requires_provenance(self):
        with pytest.raises(ValidationError, match="review_id or event_id"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                reward_source="node_execution",
                node_role="worker",
                success=True,
                current_q_action=0.5,
                current_q_hypothesis=0.5,
                current_q_relevance=0.5,
            )

    def test_reward_source_requires_current_q_baseline(self):
        with pytest.raises(ValidationError, match="current_q_action"):
            UpdateSynapseQPayload(
                synapse_id="syn-1",
                reward_source="node_execution",
                node_role="worker",
                success=True,
                event_id="evt-1",
            )

    def test_reward_source_valid_with_explicit_current_q_baseline(self):
        payload = UpdateSynapseQPayload(
            synapse_id="syn-1",
            reward_source="node_execution",
            node_role="worker",
            success=True,
            event_id="evt-1",
            current_q_action=0.6,
            current_q_hypothesis=0.5,
            current_q_relevance=0.4,
        )
        assert payload.current_q_action == 0.6
        assert payload.current_q_hypothesis == 0.5
        assert payload.current_q_relevance == 0.4


class TestSynapseTenantMismatchFaultClassification:
    """SynapseTenantMismatchError must classify as policy_fault through the
    shared contextunity.core.faults taxonomy, not just carry its own
    hardcoded attribute — proves the two are actually wired together."""

    def test_classifies_as_policy_fault(self):
        from contextunity.core.faults import classify_exception

        assert classify_exception(SynapseTenantMismatchError()) == "policy_fault"

    def test_does_not_penalize_q(self):
        from contextunity.core.faults import classify_exception, penalizes_agent_q

        fault_class = classify_exception(SynapseTenantMismatchError())
        assert penalizes_agent_q(fault_class) is False
