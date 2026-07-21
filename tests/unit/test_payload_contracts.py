"""Payload contract tests — bounded constraints and default verification.

Framework-level tests (required fields, extra=forbid) removed — those test
Pydantic internals, not our domain logic. We keep only tests that catch
actual regressions: bound violations and default value drift.
"""

from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from contextunity.brain.payloads import (
    AdminGetMemoryLayerStatsPayload,
    AppendConversationRecordPayload,
    GraphSearchPayload,
    LogTracePayload,
    MatchDuckDBPayload,
    PruneExpiredBlackboardPayload,
    QueryConversationHistoryPayload,
    SearchCellsPayload,
)

# ═══════════════════════════════════════════════════════════════════
# Bounded field constraints
# ═══════════════════════════════════════════════════════════════════


class TestBoundedConstraints:
    """Fields with ge/le constraints must be enforced."""

    def test_graph_search_max_hops_lower_bound(self):
        with pytest.raises(ValidationError, match="max_hops"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 0}
            )

    def test_graph_search_max_hops_upper_bound(self):
        with pytest.raises(ValidationError, match="max_hops"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 100}
            )

    def test_graph_search_max_results_upper_bound(self):
        with pytest.raises(ValidationError, match="max_results"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_results": 5000}
            )

    def test_graph_search_valid_bounds(self):
        payload = GraphSearchPayload.model_validate(
            {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 5, "max_results": 500}
        )
        assert payload.max_hops == 5
        assert payload.max_results == 500


# ═══════════════════════════════════════════════════════════════════
# Default value verification
# ═══════════════════════════════════════════════════════════════════


class TestDefaults:
    """Catch default value drift that silently changes query behavior."""

    def test_search_cells_defaults(self):
        p = SearchCellsPayload.model_validate({"tenant_id": "t", "query_text": "hello"})
        assert p.limit == 10
        assert p.min_score == 0.0
        assert p.source_types == []
        assert p.metadata_filter == {}

    def test_search_cells_metadata_filter_is_closed_and_bounded(self) -> None:
        payload = SearchCellsPayload.model_validate(
            {
                "tenant_id": "t",
                "query_text": "hello",
                "metadata_filter": {"service": "contextunity.docs", "doc_type": "documentation"},
            }
        )
        assert payload.metadata_filter == {
            "doc_type": "documentation",
            "service": "contextunity.docs",
        }
        with pytest.raises(ValidationError, match="metadata_filter"):
            SearchCellsPayload.model_validate(
                {"tenant_id": "t", "query_text": "hello", "metadata_filter": {"secret": "x"}}
            )

    def test_prune_expired_blackboard_requires_tenant_scope(self):
        payload = PruneExpiredBlackboardPayload.model_validate({"tenant_id": "tenant-a"})
        assert payload.tenant_id == "tenant-a"
        with pytest.raises(ValidationError, match="tenant_id"):
            PruneExpiredBlackboardPayload.model_validate({"tenant_id": ""})

    def test_memory_stats_layer_rejects_unknown_names(self) -> None:
        assert AdminGetMemoryLayerStatsPayload(layer="cells").layer == "cells"
        with pytest.raises(ValidationError, match="layer"):
            AdminGetMemoryLayerStatsPayload(layer="knowledge_nodes")


class TestConversationHistoryPayloads:
    """Canonical history contracts are closed, bounded, and provenance-complete."""

    def test_append_requires_canonical_hashes_and_idempotency(self) -> None:
        payload = AppendConversationRecordPayload.model_validate(
            {
                "record_id": "11111111-1111-4111-8111-111111111111",
                "tenant_id": "tenant-a",
                "user_id": "user-a",
                "session_id": "session-a",
                "role": "user",
                "kind": "message",
                "content": "hello",
                "content_hash": "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                "source_hash": "sha256:" + "b" * 64,
                "graph_run_id": "22222222-2222-4222-8222-222222222222",
                "metadata_version": 1,
                "idempotency_key": "router:session-a:turn-1",
            }
        )

        assert payload.metadata_version == 1
        assert payload.content_hash == (
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )

    def test_append_rejects_unknown_or_raw_fields(self) -> None:
        with pytest.raises(ValidationError, match="raw_response"):
            AppendConversationRecordPayload.model_validate(
                {
                    "record_id": "11111111-1111-4111-8111-111111111111",
                    "tenant_id": "tenant-a",
                    "user_id": "user-a",
                    "role": "user",
                    "kind": "message",
                    "content": "hello",
                    "content_hash": "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                    "source_hash": "sha256:" + "b" * 64,
                    "metadata_version": 1,
                    "idempotency_key": "router:turn-1",
                    "raw_response": "forbidden",
                }
            )

    @pytest.mark.parametrize(
        ("projection", "fields"),
        [
            ("recent", {"user_id": "user-a"}),
            ("older_than", {"older_than_days": 30}),
            ("session", {"session_id": "session-a"}),
            (
                "trace_related",
                {"graph_run_id": "22222222-2222-4222-8222-222222222222"},
            ),
        ],
    )
    def test_query_variants_require_their_exact_selector(
        self, projection: str, fields: dict[str, object]
    ) -> None:
        payload = QueryConversationHistoryPayload.model_validate(
            {"tenant_id": "tenant-a", "projection": projection, **fields}
        )
        assert payload.projection == projection

    def test_query_rejects_mixed_selectors(self) -> None:
        with pytest.raises(ValidationError, match="selector"):
            QueryConversationHistoryPayload.model_validate(
                {
                    "tenant_id": "tenant-a",
                    "projection": "recent",
                    "user_id": "user-a",
                    "session_id": "session-a",
                }
            )


_SIGV4_QUERY = (
    "X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=test%2F20260721%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260721T120000Z&X-Amz-Expires=300"
    "&X-Amz-SignedHeaders=host"
    f"&X-Amz-Signature={'a' * 64}"
)


def _object_url(path: str, *, host: str = "bucket.s3.amazonaws.com") -> str:
    return f"https://{host}/{path}?{_SIGV4_QUERY}"


class TestMatchDuckDBPayload:
    """Contract between Commerce matcher client and Brain MatchDuckDB RPC."""

    def test_accepts_current_commerce_payload(self):
        payload = MatchDuckDBPayload.model_validate(
            {
                "tenant_id": "tenant-a",
                "unmatched_url": _object_url("unmatched.parquet"),
                "canonical_url": _object_url("canonical.parquet"),
                "leftovers_put_url": _object_url("leftovers.json"),
            }
        )
        assert payload.tenant_id == "tenant-a"
        assert payload.unmatched_url.startswith("https://bucket.s3.amazonaws.com/")

    def test_accepts_legacy_url_aliases_without_extra_fields(self):
        payload = MatchDuckDBPayload.model_validate(
            {
                "tenant_id": "tenant-a",
                "url_unmatched": _object_url("unmatched.parquet"),
                "url_canonical": _object_url("canonical.parquet"),
                "url_leftovers_put": _object_url("leftovers.json"),
            }
        )
        assert payload.canonical_url.startswith("https://bucket.s3.amazonaws.com/")

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost:8000/metadata",
            _object_url("private.parquet", host="127.0.0.1"),
            "https://attacker.example/unmatched.parquet?X-Amz-Signature=fake",
            "https://bucket.s3.amazonaws.com/unmatched.parquet",
            _object_url("object", host="abc.execute-api.us-east-1.amazonaws.com"),
            _object_url("object").replace("a" * 64, "bad"),
            _object_url("object").replace("X-Amz-SignedHeaders=host", "X-Amz-SignedHeaders=x-test"),
        ],
    )
    def test_rejects_non_object_or_malformed_presigned_urls(self, url: str):
        with pytest.raises(ValueError, match="HTTPS SigV4"):
            MatchDuckDBPayload.model_validate(
                {
                    "tenant_id": "tenant-a",
                    "unmatched_url": url,
                    "canonical_url": _object_url("canonical.parquet"),
                    "leftovers_put_url": _object_url("leftovers.json"),
                }
            )


class TestTerminalTracePayload:
    """Terminal traces reject raw content and mixed legacy fields at ingress."""

    @staticmethod
    def _empty_control_evidence(
        *,
        fault_refs: list[str] | None = None,
        effect_receipt_refs: list[str] | None = None,
    ) -> dict[str, object]:
        return {
            "node_attempts": 0,
            "failed_node_attempts": 0,
            "model_attempts": 0,
            "failed_model_attempts": 0,
            "tool_attempts": 0,
            "failed_tool_attempts": 0,
            "graph_cycles": 0,
            "fault_refs": fault_refs or [],
            "effect_receipt_refs": effect_receipt_refs or [],
        }

    @staticmethod
    def _wire() -> dict[str, object]:
        return {
            "schema_version": "contextunity.execution-trace/v3",
            "trace_id": "11111111-1111-4111-8111-111111111111",
            "graph_run_id": "22222222-2222-4222-8222-222222222222",
            "tenant_id": "tenant-a",
            "agent_id": "router-agent",
            "project_id": "project-a",
            "graph_name": "graph-a",
            "terminal_status": "succeeded",
            "terminal_reason": "verified_success",
            "duration_ms": 1,
            "steps": [],
            "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
            "digest": "a" * 64,
        }

    def test_accepts_v1_terminal_reason_during_ordered_v2_rollout(self) -> None:
        wire = self._wire()
        wire["schema_version"] = "contextunity.execution-trace/v1"
        wire["terminal_reason"] = "completed"

        payload = LogTracePayload.model_validate({"terminal_trace": wire})

        assert payload.terminal_trace is not None
        assert payload.terminal_trace.schema_version == "contextunity.execution-trace/v1"

    def test_accepts_closed_guidance_evidence_and_rejects_content(self) -> None:
        wire = self._wire()
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333333",
                "kind": "model",
                "name": "test/model",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "guidance_evidence": {
                    "origin": "graph_llm_node",
                    "purpose": "agentic_reasoning",
                    "mode": "required",
                    "outcome": "applied_once",
                    "policy_version": "v1",
                    "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
                    "descriptor": {
                        "artifact_id": "core.agentic-ethos",
                        "artifact_version": "v1",
                        "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                        "release_id": "2026.07.1",
                    },
                },
            }
        ]

        payload = LogTracePayload.model_validate({"terminal_trace": wire})
        assert payload.terminal_trace is not None
        evidence = payload.terminal_trace.steps[0].guidance_evidence
        assert evidence is not None
        assert evidence.outcome == "applied_once"

        for schema_version, terminal_reason in (
            ("contextunity.execution-trace/v1", "completed"),
            ("contextunity.execution-trace/v2", "verified_success"),
        ):
            wire["schema_version"] = schema_version
            wire["terminal_reason"] = terminal_reason
            with pytest.raises(ValidationError, match="requires execution trace v3"):
                LogTracePayload.model_validate({"terminal_trace": wire})
        wire["schema_version"] = "contextunity.execution-trace/v3"
        wire["terminal_reason"] = "verified_success"

        guidance = wire["steps"][0]["guidance_evidence"]
        raw_sentinel = "RAW_GUIDANCE_SENTINEL_must_never_persist"
        guidance["content"] = raw_sentinel
        with pytest.raises(ValidationError, match="Extra inputs") as raw_error:
            LogTracePayload.model_validate({"terminal_trace": wire})
        assert raw_sentinel not in str(raw_error.value)
        guidance.pop("content")

        descriptor = guidance["descriptor"]
        assert isinstance(descriptor, dict)
        descriptor["content_digest"] = "0" * 64
        with pytest.raises(ValidationError, match="trusted release"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        descriptor["content_digest"] = (
            "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1"
        )
        descriptor["release_id"] = "untrusted"
        with pytest.raises(ValidationError, match="unknown agentic guidance release descriptor"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        descriptor["release_id"] = "2026.07.1"

        guidance["purpose"] = "classification"
        guidance["mode"] = "forbidden"
        guidance["outcome"] = "not_applicable"
        guidance["descriptor"] = None
        with pytest.raises(ValidationError, match="origin and purpose"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_v4_accepts_only_matching_raw_content_free_model_artifact_ref(self) -> None:
        wire = self._wire()
        attempt_id = "33333333-3333-4333-8333-333333333333"
        wire["schema_version"] = "contextunity.execution-trace/v4"
        wire["terminal_reason"] = "verified_success"
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": attempt_id,
                "invocation_id": "55555555-5555-4555-8555-555555555555",
                "kind": "model",
                "name": "test/model",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "guidance_evidence": {
                    "origin": "graph_llm_node",
                    "purpose": "agentic_reasoning",
                    "mode": "required",
                    "outcome": "applied_once",
                    "policy_version": "v1",
                    "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
                    "descriptor": {
                        "artifact_id": "core.agentic-ethos",
                        "artifact_version": "v1",
                        "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                        "release_id": "2026.07.1",
                    },
                },
                "artifact_ref": {
                    "artifact_id": "44444444-4444-4444-8444-444444444444",
                    "identity": {
                        "tenant_id": "tenant-a",
                        "project_id": "project-a",
                        "trace_id": wire["trace_id"],
                        "graph_run_id": wire["graph_run_id"],
                        "invocation_id": "55555555-5555-4555-8555-555555555555",
                        "provider_attempt_id": attempt_id,
                        "artifact_kind": "model_io",
                    },
                    "capture_state": "captured",
                    "storage_state": "hot",
                    "content_digest": "hmac-sha256:" + "a" * 64,
                    "request_bytes": 4,
                    "response_bytes": 2,
                },
            }
        ]

        payload = LogTracePayload.model_validate({"terminal_trace": wire})
        assert payload.terminal_trace is not None
        assert payload.terminal_trace.steps[0].artifact_ref is not None

        raw_sentinel = "RAW_MODEL_CONTENT_must_not_enter_trace"
        wire["steps"][0]["artifact_ref"]["content"] = raw_sentinel
        with pytest.raises(ValidationError, match="Extra inputs") as exc:
            LogTracePayload.model_validate({"terminal_trace": wire})
        assert raw_sentinel not in str(exc.value)
        wire["steps"][0]["artifact_ref"].pop("content")

        wire["steps"][0]["artifact_ref"]["identity"]["provider_attempt_id"] = (
            "66666666-6666-4666-8666-666666666666"
        )
        with pytest.raises(ValidationError, match="does not match model attempt"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        wire["steps"][0]["artifact_ref"]["identity"]["provider_attempt_id"] = attempt_id
        wire["steps"][0]["artifact_ref"]["identity"]["tenant_id"] = "tenant-b"
        with pytest.raises(ValidationError, match="terminal trace identity"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        wire["steps"][0]["artifact_ref"]["identity"]["tenant_id"] = "tenant-a"
        wire["steps"][0]["invocation_id"] = "77777777-7777-4777-8777-777777777777"
        with pytest.raises(ValidationError, match="does not match model invocation"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_v5_accepts_only_closed_control_steps_and_rejects_raw_content(self) -> None:
        wire = self._wire()
        wire["schema_version"] = "contextunity.execution-trace/v5"
        wire["control_evidence"] = self._empty_control_evidence()
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333339",
                "kind": "control",
                "name": "router_censor",
                "status": "succeeded",
                "duration_ms": 0,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "control_action": "continue",
                "control_reason": "policy_continue",
                "evidence_refs": ["policy:" + "a" * 64, "verifier:node:verifier"],
            }
        ]

        payload = LogTracePayload.model_validate({"terminal_trace": wire})
        assert payload.terminal_trace is not None
        assert payload.terminal_trace.steps[0].control_action == "continue"

        wire["schema_version"] = "contextunity.execution-trace/v4"
        with pytest.raises(ValidationError, match="control steps require exactly"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        wire["schema_version"] = "contextunity.execution-trace/v5"
        wire["steps"][0]["control_reason"] = "caller_invented_reason"
        with pytest.raises(ValidationError, match="control_reason"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        wire["steps"][0]["control_reason"] = "policy_continue"
        raw_sentinel = "RAW_CONTROL_PAYLOAD_must_never_persist"
        wire["steps"][0]["payload"] = raw_sentinel
        with pytest.raises(ValidationError, match="Extra inputs") as error:
            LogTracePayload.model_validate({"terminal_trace": wire})
        assert raw_sentinel not in str(error.value)

    def test_v5_rejects_raw_or_unbounded_control_evidence_refs(self) -> None:
        wire = self._wire()
        wire["schema_version"] = "contextunity.execution-trace/v5"
        wire["control_evidence"] = self._empty_control_evidence()
        raw_sentinel = "RAW_SECRET_bearer_user@example.com"
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333339",
                "kind": "control",
                "name": "router_censor",
                "status": "succeeded",
                "duration_ms": 0,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "control_action": "continue",
                "control_reason": "policy_continue",
                "evidence_refs": [raw_sentinel * 10_000],
            }
        ]

        with pytest.raises(ValidationError, match="evidence_refs") as error:
            LogTracePayload.model_validate({"terminal_trace": wire})
        assert raw_sentinel not in str(error.value)

        for raw_ref in ("email:user@example.com", "secret:sk-live-123456789"):
            wire["steps"][0]["evidence_refs"] = [raw_ref]
            with pytest.raises(ValidationError, match="unsupported closed reference") as error:
                LogTracePayload.model_validate({"terminal_trace": wire})
            assert raw_ref not in str(error.value)

    def test_v5_replan_requires_root_evidence_and_matching_lineage_refs(self) -> None:
        wire = self._wire()
        wire.update(
            {
                "schema_version": "contextunity.execution-trace/v5",
                "plan_id": "plan-1",
                "plan_revision": 1,
                "terminal_status": "failed",
                "terminal_reason": "replan_requested",
                "control_evidence": self._empty_control_evidence(
                    fault_refs=["44444444-4444-4444-8444-444444444444"],
                    effect_receipt_refs=["55555555-5555-4555-8555-555555555555"],
                ),
            }
        )
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333339",
                "kind": "control",
                "name": "router_censor",
                "status": "succeeded",
                "duration_ms": 0,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "control_action": "request_replan",
                "control_reason": "stagnation_detected",
                "evidence_refs": [
                    "policy:" + "a" * 64,
                    "verifier:node:verifier",
                    "fault:44444444-4444-4444-8444-444444444444",
                    "effect:55555555-5555-4555-8555-555555555555",
                ],
                "replan_request": {
                    "run_id": wire["graph_run_id"],
                    "reason": "stagnation_detected",
                    "verifier_ref": "node:verifier",
                    "policy_digest": "a" * 64,
                    "plan_id": "plan-1",
                    "plan_revision": 1,
                    "failed_task_ids": [],
                    "stalled_task_ids": [],
                    "remaining_provider_attempts": 1,
                    "remaining_node_attempts": 1,
                    "remaining_graph_cycles": 1,
                    "remaining_side_effect_attempts": 1,
                    "remaining_input_tokens": 1,
                    "remaining_output_tokens": 1,
                    "remaining_cost_micros": 1,
                    "remaining_wall_time_ms": 1,
                    "fault_refs": ["44444444-4444-4444-8444-444444444444"],
                    "effect_receipt_refs": ["55555555-5555-4555-8555-555555555555"],
                    "progress_hashes": [],
                    "stagnation_hashes": ["b" * 64],
                },
            }
        ]
        LogTracePayload.model_validate({"terminal_trace": wire})

        control = wire.pop("control_evidence")
        with pytest.raises(ValidationError, match="control evidence"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        wire["control_evidence"] = control

        replan = wire["steps"][0]["replan_request"]
        replan["plan_id"] = "other-plan"
        with pytest.raises(ValidationError, match="plan lineage"):
            LogTracePayload.model_validate({"terminal_trace": wire})
        replan["plan_id"] = "plan-1"

        replan["fault_refs"] = ["66666666-6666-4666-8666-666666666666"]
        with pytest.raises(ValidationError, match="fault refs"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_v3_rejects_guidance_on_non_model_and_missing_logical_evidence(self) -> None:
        wire = self._wire()
        evidence = {
            "origin": "graph_llm_node",
            "purpose": "agentic_reasoning",
            "mode": "required",
            "outcome": "applied_once",
            "policy_version": "v1",
            "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
            "descriptor": {
                "artifact_id": "core.agentic-ethos",
                "artifact_version": "v1",
                "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                "release_id": "2026.07.1",
            },
        }
        step = {
            "sequence": 0,
            "attempt_id": "33333333-3333-4333-8333-333333333333",
            "kind": "node",
            "name": "test/node",
            "status": "succeeded",
            "duration_ms": 1,
            "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
            "guidance_evidence": evidence,
        }
        wire["steps"] = [step]
        with pytest.raises(ValidationError, match="allowed only on model steps"):
            LogTracePayload.model_validate({"terminal_trace": wire})

        step["kind"] = "model"
        step.pop("guidance_evidence")
        with pytest.raises(ValidationError, match="logical model step requires"):
            LogTracePayload.model_validate({"terminal_trace": wire})

        step["guidance_evidence"] = evidence
        child = {
            "sequence": 1,
            "attempt_id": "44444444-4444-4444-8444-444444444444",
            "parent_attempt_id": step["attempt_id"],
            "kind": "model",
            "name": "test/provider-child",
            "status": "succeeded",
            "duration_ms": 1,
            "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
            "guidance_evidence": evidence,
        }
        wire["steps"] = [step, child]
        with pytest.raises(ValidationError, match="fallback provider child"):
            LogTracePayload.model_validate({"terminal_trace": wire})

        child.pop("guidance_evidence")
        payload = LogTracePayload.model_validate({"terminal_trace": wire})
        assert payload.terminal_trace is not None
        assert payload.terminal_trace.steps[1].guidance_evidence is None

    def test_rejects_partial_plan_correlation(self) -> None:
        wire = self._wire()
        wire["plan_id"] = "plan-2"
        with pytest.raises(ValidationError, match="plan correlation"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_rejects_chronos_decision_from_a_different_graph_run(self) -> None:
        wire = self._wire()
        wire["control_evidence"] = {
            "node_attempts": 0,
            "failed_node_attempts": 0,
            "model_attempts": 0,
            "failed_model_attempts": 0,
            "tool_attempts": 0,
            "failed_tool_attempts": 0,
            "graph_cycles": 0,
            "contribution_refs": [],
            "invalid_contribution_refs": [],
            "fault_refs": [],
            "effect_receipt_refs": [],
            "effect_receipts": [],
            "graph_cycle_refs": [],
            "chronos_decisions": [
                {
                    "run_id": "33333333-3333-4333-8333-333333333333",
                    "sequence": 0,
                    "kind": "run_started",
                    "elapsed_ms": 0,
                    "deadline_ms": 1,
                }
            ],
        }
        with pytest.raises(ValidationError, match="RouterChronos decision run"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_rejects_raw_prompt_content(self):
        wire = self._wire()
        wire["raw_prompt"] = "never store me"
        with pytest.raises(ValidationError, match="raw_prompt"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    @pytest.mark.parametrize(
        "unsafe_preview",
        [
            "Patient Oleksii lives at 10 Main Street and has HIV.",
            "Call +380 67 123 45 67",
            "arbitrary free-form user content",
        ],
    )
    def test_rejects_free_form_user_prompt_preview(self, unsafe_preview: str) -> None:
        wire = self._wire()
        wire["prompt_evidence"] = [
            {
                "role": "user",
                "redacted_preview": unsafe_preview,
                "redaction_policy_version": "contextunity.prompt-redaction/v1",
            }
        ]

        with pytest.raises(ValidationError, match="closed redacted preview"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    @staticmethod
    def _control_evidence() -> dict[str, object]:
        receipt_id = "44444444-4444-4444-8444-444444444444"
        return {
            "node_attempts": 1,
            "failed_node_attempts": 0,
            "model_attempts": 0,
            "failed_model_attempts": 0,
            "tool_attempts": 1,
            "failed_tool_attempts": 0,
            "graph_cycles": 0,
            "contribution_refs": [],
            "invalid_contribution_refs": [],
            "fault_refs": [],
            "effect_receipt_refs": [receipt_id],
            "effect_receipts": [
                {
                    "receipt_id": receipt_id,
                    "operation_id": "55555555-5555-4555-8555-555555555555",
                    "idempotency_key": "66666666-6666-4666-8666-666666666666",
                    "effect_state": "committed",
                    "replay_safe": False,
                    "adapter_id": "federated:write",
                    "capability_id": "federated:write",
                    "effect_or_result_hash": "b" * 64,
                }
            ],
        }

    def test_accepts_closed_brain_read_control_evidence(self) -> None:
        wire = self._wire()
        control = self._control_evidence()
        fault_ref = "77777777-7777-4777-8777-777777777777"
        control["fault_refs"] = [fault_ref]
        control["brain_reads"] = [
            {
                "query_kind": "memory_read",
                "requested_depth": "deep",
                "effective_depth": "standard",
                "outcome": "executed",
                "degraded": True,
                "queue_wait_ms": 2,
                "duration_ms": 7,
                "retryable": True,
                "fault_ref": fault_ref,
            }
        ]
        wire["control_evidence"] = control

        payload = LogTracePayload.model_validate({"terminal_trace": wire})

        assert payload.terminal_trace.control_evidence is not None
        assert payload.terminal_trace.control_evidence.brain_reads[0].outcome == "executed"

    def test_rejects_brain_read_fault_outside_control_refs(self) -> None:
        wire = self._wire()
        control = self._control_evidence()
        control["brain_reads"] = [
            {
                "query_kind": "memory_read",
                "requested_depth": "deep",
                "effective_depth": "standard",
                "outcome": "queue_full",
                "degraded": False,
                "queue_wait_ms": 2,
                "duration_ms": 7,
                "retryable": True,
                "fault_ref": "77777777-7777-4777-8777-777777777777",
            }
        ]
        wire["control_evidence"] = control

        with pytest.raises(ValidationError, match="control fault_refs"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_accepts_closed_effect_receipt_control_evidence(self) -> None:
        wire = self._wire()
        wire["control_evidence"] = self._control_evidence()
        payload = LogTracePayload.model_validate({"terminal_trace": wire})
        assert payload.terminal_trace.control_evidence is not None
        assert payload.terminal_trace.control_evidence.tool_attempts == 1

    @pytest.mark.parametrize("mutation", ["missing_ref", "unsafe_replay", "failed_over_total"])
    def test_rejects_inconsistent_effect_control_evidence(self, mutation: str) -> None:
        wire = self._wire()
        control = self._control_evidence()
        if mutation == "missing_ref":
            control["effect_receipt_refs"] = []
        elif mutation == "unsafe_replay":
            receipts = control["effect_receipts"]
            assert isinstance(receipts, list)
            receipt = receipts[0]
            assert isinstance(receipt, dict)
            receipt["replay_safe"] = True
        else:
            control["failed_tool_attempts"] = 2
        wire["control_evidence"] = control
        with pytest.raises(ValidationError):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_rejects_nested_tool_payload(self):
        wire = self._wire()
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333333",
                "kind": "tool",
                "name": "search",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "tool_payload": {"secret": "value"},
            }
        ]
        with pytest.raises(ValidationError, match="tool_payload"):
            LogTracePayload.model_validate({"terminal_trace": wire})

    def test_rejects_legacy_open_fields_alongside_terminal_trace(self):
        with pytest.raises(ValidationError, match="legacy open-content"):
            LogTracePayload.model_validate(
                {"terminal_trace": self._wire(), "metadata": {"raw_response": "value"}}
            )

    def test_provider_usage_details_are_validated_and_preserved_per_model_attempt(self) -> None:
        wire = self._wire()
        guidance = {
            "origin": "graph_llm_node",
            "purpose": "agentic_reasoning",
            "mode": "required",
            "outcome": "applied_once",
            "policy_version": "v1",
            "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
            "descriptor": {
                "artifact_id": "core.agentic-ethos",
                "artifact_version": "v1",
                "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                "release_id": "2026.07.1",
            },
        }
        wire["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "33333333-3333-4333-8333-333333333333",
                "kind": "model",
                "name": "test/model",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 3,
                    "cost_micros": 2,
                    "provider_details": {
                        "schema_id": "openai.responses.usage/v1",
                        "values": {
                            "openai.cached_input_tokens": 7,
                            "openai.reasoning_output_tokens": 2,
                        },
                    },
                },
                "guidance_evidence": guidance,
            }
        ]
        wire["usage"] = {"input_tokens": 10, "output_tokens": 3, "cost_micros": 2}

        payload = LogTracePayload.model_validate({"terminal_trace": wire})

        terminal = payload.terminal_trace
        assert terminal is not None
        details = terminal.steps[0].usage.provider_details
        assert details is not None
        assert details.values["openai.cached_input_tokens"] == 7

        wire_encoded = deepcopy(wire)
        encoded_step = wire_encoded["steps"][0]
        encoded_step["usage"]["provider_details"]["values"] = {
            "openai.cached_input_tokens": "7",
            "openai.reasoning_output_tokens": "2",
        }
        encoded = LogTracePayload.model_validate({"terminal_trace": wire_encoded})
        assert encoded.terminal_trace is not None
        assert encoded.terminal_trace.steps[0].usage.provider_details is not None
        assert encoded.terminal_trace.steps[0].usage.provider_details.values == {
            "openai.cached_input_tokens": 7,
            "openai.reasoning_output_tokens": 2,
        }

        float_wire = deepcopy(wire)
        float_step = float_wire["steps"][0]
        float_step["usage"]["provider_details"]["values"] = {
            "openai.cached_input_tokens": 7.0,
        }
        with pytest.raises(ValidationError):
            LogTracePayload.model_validate({"terminal_trace": float_wire})

        invalid = self._wire()
        invalid["steps"] = list(wire["steps"])
        invalid["usage"] = wire["usage"]
        invalid_step = invalid["steps"][0]
        assert isinstance(invalid_step, dict)
        invalid_usage = invalid_step["usage"]
        assert isinstance(invalid_usage, dict)
        invalid_details = invalid_usage["provider_details"]
        assert isinstance(invalid_details, dict)
        invalid_details["schema_id"] = "unknown.usage/v1"
        with pytest.raises(ValidationError, match="unknown provider usage schema"):
            LogTracePayload.model_validate({"terminal_trace": invalid})
