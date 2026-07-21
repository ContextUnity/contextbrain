"""Payloads for Brain memory, execution traces, and blackboard operations."""

import hashlib
import ipaddress
import json
import re
from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Literal
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

from contextunity.core.sdk.agentic_guidance import AgenticGuidanceEvidence
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactIdentity,
    ExecutionTraceArtifactRef,
    ModelIOContentPart,
    ModelIOProviderStatus,
)
from contextunity.core.sdk.provider_usage import (
    ProviderUsageDetails,
    trusted_provider_usage_schema,
)
from contextunity.core.sdk.types import (
    USER_PROMPT_REDACTED_PREVIEW,
    BrainReadDepth,
    BrainReadEvidenceOutcome,
    BrainReadKind,
    StrictPayloadModel,
    TraceControlAction,
    TraceControlReason,
)
from contextunity.core.types import JsonDict, is_object_dict
from pydantic import Field, StringConstraints, field_validator, model_validator

# Conversation History
# =====================================================

_SHA256_PATTERN = r"sha256:[0-9a-f]{64}"

CanonicalEvidenceRef = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@/-]*$",
    ),
]
_EVIDENCE_REF_PATTERNS = (
    re.compile(r"policy:[0-9a-f]{64}"),
    re.compile(r"verifier:node:[A-Za-z0-9][A-Za-z0-9._:@/-]{0,110}"),
    re.compile(
        r"(?:fault|effect|synapse):[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    ),
)


class AppendConversationRecordPayload(StrictPayloadModel):
    """Closed canonical record accepted by ``AppendConversationRecord``."""

    record_id: UUID
    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=256)
    session_id: str | None = Field(default=None, max_length=256)
    role: Literal["user", "assistant", "system", "tool"]
    kind: Literal["message", "turn_summary", "conversation_note"]
    content: str = Field(min_length=1, max_length=65536)
    content_hash: str = Field(pattern=_SHA256_PATTERN)
    source_hash: str = Field(pattern=_SHA256_PATTERN)
    graph_run_id: UUID | None = None
    metadata_version: Literal[1] = 1
    idempotency_key: str = Field(min_length=1, max_length=256)
    metadata: JsonDict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_content_hash(self) -> "AppendConversationRecordPayload":
        actual = "sha256:" + hashlib.sha256(self.content.encode("utf-8")).hexdigest()
        if self.content_hash != actual:
            raise ValueError("content_hash does not match content")
        return self


class QueryConversationHistoryPayload(StrictPayloadModel):
    """One bounded query contract with a closed projection selector."""

    tenant_id: str = Field(min_length=1, max_length=128)
    projection: Literal["recent", "older_than", "session", "trace_related"]
    user_id: str | None = Field(default=None, max_length=256)
    session_id: str | None = Field(default=None, max_length=256)
    graph_run_id: UUID | None = None
    older_than_days: int | None = Field(default=None, ge=0, le=36500)
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0, le=1_000_000)

    @model_validator(mode="after")
    def _validate_projection_selector(self) -> "QueryConversationHistoryPayload":
        selectors = {
            "recent": self.user_id,
            "older_than": self.older_than_days,
            "session": self.session_id,
            "trace_related": self.graph_run_id,
        }
        selected = selectors[self.projection]
        supplied = sum(value is not None for value in selectors.values())
        if selected is None or supplied != 1:
            raise ValueError("conversation query requires exactly one projection selector")
        return self


class GetConversationHistoryStatsPayload(StrictPayloadModel):
    """Tenant-scoped bounded Conversation History statistics request."""

    tenant_id: str = Field(min_length=1, max_length=128)


class ApplyConversationRetentionPayload(StrictPayloadModel):
    """Fail-closed owner retention request with explicit policy evidence."""

    tenant_id: str = Field(min_length=1, max_length=128)
    record_ids: list[UUID] = Field(min_length=1, max_length=500)
    cutoff: datetime
    policy_version: Literal["contextunity.conversation-retention/v1"]
    hold_evidence_hash: str = Field(pattern=_SHA256_PATTERN)


class ApplyExecutionTraceRetentionPayload(StrictPayloadModel):
    """Tenant-scoped age policy for terminal Execution Trace retention."""

    tenant_id: str = Field(min_length=1, max_length=128)
    older_than_days: int = Field(default=30, ge=0, le=36500)


# =====================================================
# Protected Execution Trace artifacts
# =====================================================


class ReserveExecutionTraceArtifactPayload(StrictPayloadModel):
    """Request reservation protected before provider egress."""

    identity: ExecutionTraceArtifactIdentity
    artifact_id: UUID
    lifecycle_profile_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )
    capture_policy_version: Literal["contextunity.model-io-capture/v1"]
    request_parts: list[ModelIOContentPart] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_request_order(self) -> "ReserveExecutionTraceArtifactPayload":
        if [part.sequence for part in self.request_parts] != list(range(len(self.request_parts))):
            raise ValueError("model I/O request part order must be contiguous")
        return self


class FinalizeExecutionTraceArtifactPayload(StrictPayloadModel):
    """Terminal visible response used to finalize one reservation by CAS."""

    identity: ExecutionTraceArtifactIdentity
    artifact_id: UUID
    expected_revision: int = Field(ge=1)
    provider_status: ModelIOProviderStatus
    response_parts: list[ModelIOContentPart] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def validate_response(self) -> "FinalizeExecutionTraceArtifactPayload":
        if [part.sequence for part in self.response_parts] != list(range(len(self.response_parts))):
            raise ValueError("model I/O response part order must be contiguous")
        if self.provider_status == "succeeded" and not self.response_parts:
            raise ValueError("successful model I/O requires a visible response part")
        return self


class GetExecutionTraceArtifactPayload(StrictPayloadModel):
    """Exact tenant/project-scoped protected artifact read."""

    tenant_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    artifact_id: UUID


class ArchiveExecutionTraceArtifactPayload(GetExecutionTraceArtifactPayload):
    """Move one finalized hot artifact to its C0-selected cold profile by CAS."""

    expected_revision: int = Field(ge=2)
    lifecycle_profile_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )


class RestoreExecutionTraceArtifactPayload(GetExecutionTraceArtifactPayload):
    """Restore one archived artifact to hot ciphertext storage by CAS."""

    expected_revision: int = Field(ge=3)
    lifecycle_profile_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )


class PurgeExecutionTraceArtifactPayload(GetExecutionTraceArtifactPayload):
    """CAS purge request; lifecycle legal hold remains C0-owned."""

    expected_revision: int = Field(ge=1)
    lifecycle_profile_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )


# Agent Traces
# =====================================================


class TraceUsagePayload(StrictPayloadModel):
    """Router-normalized token and cost accounting."""

    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cost_micros: int = Field(ge=0)
    provider_details: ProviderUsageDetails | None = None

    @field_validator("provider_details", mode="before")
    @classmethod
    def normalize_provider_detail_wire(cls, value: object) -> object:
        """Narrow canonical decimal uint64 strings after ContextUnit transport."""
        if value is None or isinstance(value, ProviderUsageDetails):
            return value
        if not is_object_dict(value):
            return value
        values = value.get("values")
        if not is_object_dict(values):
            return value
        normalized: dict[str, int] = {}
        for key, counter in values.items():
            if isinstance(counter, bool):
                return value
            if isinstance(counter, int):
                normalized[key] = counter
                continue
            if (
                isinstance(counter, str)
                and counter
                and counter.isascii()
                and counter.isdecimal()
                and (counter == "0" or not counter.startswith("0"))
            ):
                normalized[key] = int(counter)
                continue
            return value
        return {"schema_id": value.get("schema_id"), "values": normalized}

    @model_validator(mode="after")
    def validate_provider_details(self) -> "TraceUsagePayload":
        details = self.provider_details
        if details is not None:
            schema = trusted_provider_usage_schema(details.schema_id)
            details.validate_against(
                schema,
                input_tokens=self.input_tokens,
                output_tokens=self.output_tokens,
            )
        return self


class PromptEvidencePayload(StrictPayloadModel):
    """Bounded preview or exact registered-prompt reference."""

    role: Literal["user", "system"]
    prompt_ref: str | None = None
    prompt_version: str | None = None
    redacted_preview: str = Field(max_length=512)
    redaction_policy_version: Literal["contextunity.prompt-redaction/v1"]

    @model_validator(mode="after")
    def _validate_prompt_evidence(self) -> "PromptEvidencePayload":
        if (self.prompt_ref is None) != (self.prompt_version is None):
            raise ValueError("prompt_ref and prompt_version must be provided together")
        if self.role == "system" and self.prompt_ref is None:
            raise ValueError("system prompt evidence requires exact ref and version")
        if self.role == "user" and self.redacted_preview != USER_PROMPT_REDACTED_PREVIEW:
            raise ValueError("user prompt evidence must use the closed redacted preview")
        sensitive = re.compile(
            r"(?i)(bearer\s+\S+|(?:api[_-]?key|token|password|secret)\s*[:=]\s*\S+|"
            r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})"
        )
        if sensitive.search(self.redacted_preview):
            raise ValueError("redacted preview contains sensitive content")
        return self


class TraceReplanRequestPayload(StrictPayloadModel):
    """Terminal bounded Phase-4 evidence for a future replanning authority."""

    run_id: UUID
    reason: TraceControlReason
    verifier_ref: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@/-]*$",
    )
    policy_digest: str = Field(pattern=r"[0-9a-f]{64}")
    plan_id: str | None = Field(default=None, min_length=1, max_length=128)
    plan_revision: int | None = Field(default=None, ge=0)
    parent_plan_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_plan_revision: int | None = Field(default=None, ge=0)
    prior_replan_ref: UUID | None = None
    failed_task_ids: list[str] = Field(default_factory=list, max_length=64)
    stalled_task_ids: list[str] = Field(default_factory=list, max_length=64)
    remaining_provider_attempts: int = Field(ge=0)
    remaining_node_attempts: int = Field(ge=0)
    remaining_graph_cycles: int = Field(ge=0)
    remaining_side_effect_attempts: int = Field(ge=0)
    remaining_input_tokens: int = Field(ge=0)
    remaining_output_tokens: int = Field(ge=0)
    remaining_cost_micros: int = Field(ge=0)
    remaining_wall_time_ms: int = Field(ge=0)
    fault_refs: list[UUID] = Field(default_factory=list, max_length=64)
    effect_receipt_refs: list[UUID] = Field(default_factory=list, max_length=64)
    progress_hashes: list[str] = Field(
        default_factory=list,
        max_length=64,
    )
    stagnation_hashes: list[str] = Field(
        default_factory=list,
        max_length=64,
    )

    @model_validator(mode="after")
    def validate_replan_shape(self) -> "TraceReplanRequestPayload":
        plan_values = (self.plan_id, self.plan_revision)
        if any(value is not None for value in plan_values) and any(
            value is None for value in plan_values
        ):
            raise ValueError("replan plan correlation requires id and revision")
        parent_values = (
            self.parent_plan_id,
            self.parent_plan_revision,
            self.prior_replan_ref,
        )
        if any(value is not None for value in parent_values) and any(
            value is None for value in parent_values
        ):
            raise ValueError("replan parent lineage requires id, revision, and ref")
        if self.parent_plan_id is not None and self.plan_id is None:
            raise ValueError("replan parent lineage requires current plan correlation")
        identifier = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]*")
        for task_id in (*self.failed_task_ids, *self.stalled_task_ids):
            if len(task_id) > 128 or identifier.fullmatch(task_id) is None:
                raise ValueError("replan task ids must be closed identifiers")
        for digest in (*self.progress_hashes, *self.stagnation_hashes):
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("replan progress evidence must be SHA-256 digests")
        return self


class TraceStepPayload(StrictPayloadModel):
    """Closed ordered execution step without request/result content."""

    sequence: int = Field(ge=0)
    attempt_id: UUID
    invocation_id: UUID | None = None
    parent_attempt_id: UUID | None = None
    kind: Literal["node", "model", "tool", "control"]
    name: str = Field(min_length=1, max_length=128, pattern=r"[A-Za-z0-9][A-Za-z0-9._:@/-]*")
    status: Literal["succeeded", "failed", "cancelled"]
    duration_ms: int = Field(ge=0)
    usage: TraceUsagePayload
    prompt_evidence: PromptEvidencePayload | None = None
    guidance_evidence: AgenticGuidanceEvidence | None = None
    artifact_ref: ExecutionTraceArtifactRef | None = None
    error_code: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"[A-Za-z0-9][A-Za-z0-9._:@/-]*",
    )
    control_action: TraceControlAction | None = None
    control_reason: TraceControlReason | None = None
    evidence_refs: list[CanonicalEvidenceRef] = Field(default_factory=list, max_length=64)
    replan_request: TraceReplanRequestPayload | None = None

    @field_validator("evidence_refs")
    @classmethod
    def validate_evidence_ref_kinds(
        cls,
        refs: list[CanonicalEvidenceRef],
    ) -> list[CanonicalEvidenceRef]:
        if any(
            not any(pattern.fullmatch(ref) for pattern in _EVIDENCE_REF_PATTERNS) for ref in refs
        ):
            raise ValueError("control evidence ref uses an unsupported closed reference kind")
        return refs

    @model_validator(mode="after")
    def validate_guidance_kind(self) -> "TraceStepPayload":
        """Reject model-policy evidence on node or tool attempts."""
        if self.kind != "model" and self.guidance_evidence is not None:
            raise ValueError("guidance evidence is allowed only on model steps")
        if self.kind != "model" and self.usage.provider_details is not None:
            raise ValueError("provider usage details are allowed only on model steps")
        if self.kind != "model" and self.artifact_ref is not None:
            raise ValueError("protected artifact refs are allowed only on model steps")
        if self.artifact_ref is not None:
            if self.artifact_ref.identity.provider_attempt_id != self.attempt_id:
                raise ValueError("protected artifact ref does not match model attempt")
            if self.invocation_id != self.artifact_ref.identity.invocation_id:
                raise ValueError("protected artifact ref does not match model invocation")
        if self.status == "succeeded" and self.error_code is not None:
            raise ValueError("successful trace step cannot carry an error code")
        control_fields = self.control_action is not None or self.control_reason is not None
        if self.kind == "control":
            if not control_fields or self.control_action is None or self.control_reason is None:
                raise ValueError("control step requires action and reason")
            if not self.evidence_refs:
                raise ValueError("control step requires evidence refs")
            if (self.control_action == "request_replan") != (self.replan_request is not None):
                raise ValueError("request_replan action requires exactly one replan request")
            if (
                self.replan_request is not None
                and self.replan_request.reason != self.control_reason
            ):
                raise ValueError("replan request reason does not match control reason")
        elif control_fields or self.evidence_refs or self.replan_request is not None:
            raise ValueError("control fields are allowed only on control steps")
        return self


def validate_guidance_step_placement(steps: Sequence[TraceStepPayload]) -> None:
    """Reject guidance outside logical model attempts in a v3 Trace."""
    steps_by_id = {step.attempt_id: step for step in steps}
    for step in steps:
        evidence = step.guidance_evidence
        if step.kind != "model":
            if evidence is not None:
                raise ValueError("guidance evidence is allowed only on model steps")
            continue
        parent = (
            steps_by_id.get(step.parent_attempt_id) if step.parent_attempt_id is not None else None
        )
        is_fallback_child = parent is not None and parent.kind == "model"
        if is_fallback_child and evidence is not None:
            raise ValueError("fallback provider child must reuse logical guidance evidence")
        if not is_fallback_child and evidence is None:
            raise ValueError("logical model step requires guidance evidence")


class ToolEffectReceiptPayload(StrictPayloadModel):
    """Closed terminal projection of one Router-owned tool effect receipt."""

    receipt_id: UUID
    operation_id: UUID
    idempotency_key: UUID
    effect_state: Literal["not_started", "committed", "unknown", "compensated"]
    replay_safe: bool
    adapter_id: str = Field(min_length=1, max_length=128, pattern=r"[A-Za-z0-9][A-Za-z0-9._:@/-]*")
    capability_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"[A-Za-z0-9][A-Za-z0-9._:@/-]*",
    )
    effect_or_result_hash: str = Field(pattern=r"[0-9a-f]{64}")

    @model_validator(mode="after")
    def _validate_replay_safety(self) -> "ToolEffectReceiptPayload":
        if self.replay_safe and self.effect_state != "not_started":
            raise ValueError("only not_started effects may be replay-safe")
        return self


class GraphCycleEvidencePayload(StrictPayloadModel):
    """One graph-cycle identity, distinct from node-attempt evidence."""

    cycle_id: UUID
    sequence: int = Field(ge=0)


class ChronosDecisionPayload(StrictPayloadModel):
    """Closed in-GraphRun RouterChronos decision."""

    run_id: UUID
    sequence: int = Field(ge=0)
    kind: Literal[
        "run_started",
        "schedule_eligible",
        "deadline_expired",
        "node_attempt_budget_exhausted",
        "graph_cycle_budget_exhausted",
    ]
    elapsed_ms: int = Field(ge=0)
    deadline_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_deadline_decision(self) -> "ChronosDecisionPayload":
        if self.kind == "deadline_expired" and self.elapsed_ms < self.deadline_ms:
            raise ValueError("deadline decision precedes resolved deadline")
        return self


class BrainReadEvidencePayload(StrictPayloadModel):
    """Closed raw-content-free Brain-read control outcome."""

    query_kind: BrainReadKind
    requested_depth: BrainReadDepth
    effective_depth: BrainReadDepth
    outcome: BrainReadEvidenceOutcome
    degraded: bool
    queue_wait_ms: int = Field(ge=0)
    duration_ms: int = Field(ge=0)
    retryable: bool
    fault_ref: UUID | None = None

    @model_validator(mode="after")
    def _validate_fault_reference(self) -> "BrainReadEvidencePayload":
        negative = self.degraded or self.outcome in (
            "queue_full",
            "deadline_exceeded",
            "brain_unavailable",
        )
        if negative != (self.fault_ref is not None):
            raise ValueError("negative Brain-read outcomes require exactly one fault_ref")
        return self


class TraceControlEvidencePayload(StrictPayloadModel):
    """Bounded raw-content-free control evidence persisted with one Trace."""

    node_attempts: int = Field(ge=0)
    failed_node_attempts: int = Field(ge=0)
    model_attempts: int = Field(ge=0)
    failed_model_attempts: int = Field(ge=0)
    tool_attempts: int = Field(ge=0)
    failed_tool_attempts: int = Field(ge=0)
    graph_cycles: int = Field(ge=0)
    contribution_refs: list[UUID] = Field(default_factory=list, max_length=64)
    invalid_contribution_refs: list[UUID] = Field(default_factory=list, max_length=64)
    fault_refs: list[UUID] = Field(default_factory=list, max_length=64)
    effect_receipt_refs: list[UUID] = Field(default_factory=list, max_length=64)
    effect_receipts: list[ToolEffectReceiptPayload] = Field(default_factory=list, max_length=64)
    fan_in_refs: list[UUID] = Field(default_factory=list, max_length=64)
    graph_cycle_refs: list[GraphCycleEvidencePayload] = Field(default_factory=list, max_length=64)
    chronos_decisions: list[ChronosDecisionPayload] = Field(default_factory=list, max_length=64)
    brain_reads: list[BrainReadEvidencePayload] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def _validate_control_evidence(self) -> "TraceControlEvidencePayload":
        attempt_groups = (
            (self.node_attempts, self.failed_node_attempts),
            (self.model_attempts, self.failed_model_attempts),
            (self.tool_attempts, self.failed_tool_attempts),
        )
        if any(failed > total for total, failed in attempt_groups):
            raise ValueError("failed attempts cannot exceed total attempts")
        ref_groups = (
            self.contribution_refs,
            self.invalid_contribution_refs,
            self.fault_refs,
            self.effect_receipt_refs,
            self.fan_in_refs,
        )
        if any(len(refs) != len(set(refs)) for refs in ref_groups):
            raise ValueError("control evidence references must be unique")
        receipt_ids = [receipt.receipt_id for receipt in self.effect_receipts]
        idempotency_keys = [receipt.idempotency_key for receipt in self.effect_receipts]
        if len(receipt_ids) != len(set(receipt_ids)) or len(idempotency_keys) != len(
            set(idempotency_keys)
        ):
            raise ValueError("effect receipts must have unique receipt and idempotency identities")
        if not set(receipt_ids).issubset(self.effect_receipt_refs):
            raise ValueError("effect receipt payload is missing its control evidence reference")
        cycle_ids = [cycle.cycle_id for cycle in self.graph_cycle_refs]
        if len(cycle_ids) != len(set(cycle_ids)):
            raise ValueError("graph cycle identities must be unique")
        if self.graph_cycle_refs and len(self.graph_cycle_refs) != self.graph_cycles:
            raise ValueError("graph cycle identity count does not match graph_cycles")
        decision_sequences = [decision.sequence for decision in self.chronos_decisions]
        if decision_sequences != list(range(len(decision_sequences))):
            raise ValueError("RouterChronos decisions must have contiguous canonical order")
        if any(
            evidence.fault_ref is not None and evidence.fault_ref not in self.fault_refs
            for evidence in self.brain_reads
        ):
            raise ValueError("Brain-read fault_ref must be present in control fault_refs")
        return self


class FinalVerdictPayload(StrictPayloadModel):
    """Brain-validated terminal decision; contains no Q mutation instruction."""

    verdict_digest: str = Field(pattern=r"[0-9a-f]{64}")
    terminal_status: Literal["succeeded", "failed", "cancelled"]
    terminal_reason: Literal[
        "verified_success",
        "failed",
        "blocked",
        "budget_exhausted",
        "cancelled",
        "human_review_required",
        "replan_requested",
    ]
    verifier_ref: str = Field(min_length=1, max_length=128)
    verifier_evidence_refs: list[str] = Field(min_length=1, max_length=64)
    fault_class: (
        Literal["agent_fault", "infra_fault", "upstream_fault", "policy_fault", "reference_fault"]
        | None
    ) = None
    attribution_candidates: list[UUID] = Field(default_factory=list, max_length=64)
    node_attempts: int = Field(ge=0)
    model_attempts: int = Field(ge=0)
    tool_attempts: int = Field(ge=0)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cost_micros: int = Field(ge=0)
    duration_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def _bounded_refs(self) -> "FinalVerdictPayload":
        if len(self.verifier_evidence_refs) != len(set(self.verifier_evidence_refs)):
            raise ValueError("FinalVerdict verifier refs must be unique")
        if len(self.attribution_candidates) != len(set(self.attribution_candidates)):
            raise ValueError("FinalVerdict attribution candidates must be unique")
        return self


class TerminalExecutionTracePayload(StrictPayloadModel):
    """Closed terminal snapshot accepted by Brain durable finalization."""

    schema_version: Literal[
        "contextunity.execution-trace/v1",
        "contextunity.execution-trace/v2",
        "contextunity.execution-trace/v3",
        "contextunity.execution-trace/v4",
        "contextunity.execution-trace/v5",
        "contextunity.execution-trace/v6",
    ]
    trace_id: UUID
    graph_run_id: UUID
    tenant_id: str = Field(min_length=1, max_length=128)
    agent_id: str = Field(min_length=1, max_length=128)
    session_id: str | None = None
    user_id: str | None = None
    project_id: str = Field(min_length=1, max_length=128)
    graph_name: str = Field(min_length=1, max_length=128)
    registration_hash: str | None = Field(default=None, pattern=r"[0-9a-f]{64}")
    plan_id: str | None = Field(default=None, min_length=1, max_length=128)
    plan_revision: int | None = Field(default=None, ge=0)
    parent_plan_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_plan_revision: int | None = Field(default=None, ge=0)
    replan_ref: UUID | None = None
    terminal_status: Literal["succeeded", "failed", "cancelled"]
    terminal_reason: Literal[
        "completed",
        "execution_error",
        "security_rejected",
        "verified_success",
        "failed",
        "blocked",
        "budget_exhausted",
        "cancelled",
        "human_review_required",
        "replan_requested",
    ]
    duration_ms: int = Field(ge=0)
    steps: list[TraceStepPayload] = Field(max_length=4096)
    usage: TraceUsagePayload
    prompt_evidence: list[PromptEvidencePayload] = Field(default_factory=list, max_length=64)
    provenance: list[str] = Field(default_factory=list, max_length=64)
    security_flags: list[str] = Field(default_factory=list, max_length=64)
    control_evidence: TraceControlEvidencePayload | None = None
    final_verdict: FinalVerdictPayload | None = None
    digest: str = Field(pattern=r"[0-9a-f]{64}")

    @model_validator(mode="after")
    def _validate_terminal_shape(self) -> "TerminalExecutionTracePayload":
        if self.usage.provider_details is not None:
            raise ValueError("terminal aggregate usage cannot carry one provider schema")
        if [step.sequence for step in self.steps] != list(range(len(self.steps))):
            raise ValueError("trace steps must have contiguous canonical order")
        step_ids = {step.attempt_id for step in self.steps}
        if any(
            step.parent_attempt_id is not None and step.parent_attempt_id not in step_ids
            for step in self.steps
        ):
            raise ValueError("trace step parent is missing")
        if sum(step.usage.input_tokens for step in self.steps) != self.usage.input_tokens:
            raise ValueError("trace input token total does not match steps")
        if sum(step.usage.output_tokens for step in self.steps) != self.usage.output_tokens:
            raise ValueError("trace output token total does not match steps")
        if sum(step.usage.cost_micros for step in self.steps) != self.usage.cost_micros:
            raise ValueError("trace cost total does not match steps")
        plan_values = (self.plan_id, self.plan_revision)
        if any(value is not None for value in plan_values) and any(
            value is None for value in plan_values
        ):
            raise ValueError("plan correlation requires plan_id and plan_revision")
        parent_values = (self.parent_plan_id, self.parent_plan_revision, self.replan_ref)
        if any(value is not None for value in parent_values) and any(
            value is None for value in parent_values
        ):
            raise ValueError("parent plan lineage requires id, revision, and replan ref")
        if self.parent_plan_id is not None and self.plan_id is None:
            raise ValueError("parent plan lineage requires a current plan correlation")
        if self.schema_version not in {
            "contextunity.execution-trace/v3",
            "contextunity.execution-trace/v4",
            "contextunity.execution-trace/v5",
            "contextunity.execution-trace/v6",
        } and any(step.guidance_evidence is not None for step in self.steps):
            raise ValueError("guidance evidence requires execution trace v3 or newer")
        if self.schema_version in {
            "contextunity.execution-trace/v3",
            "contextunity.execution-trace/v4",
            "contextunity.execution-trace/v5",
            "contextunity.execution-trace/v6",
        }:
            validate_guidance_step_placement(self.steps)
        if self.schema_version not in {
            "contextunity.execution-trace/v4",
            "contextunity.execution-trace/v5",
            "contextunity.execution-trace/v6",
        } and any(step.artifact_ref is not None for step in self.steps):
            raise ValueError("protected artifact refs require execution trace v4 or newer")
        has_control = any(step.kind == "control" for step in self.steps)
        is_control_schema = self.schema_version in {
            "contextunity.execution-trace/v5",
            "contextunity.execution-trace/v6",
        }
        if has_control != is_control_schema:
            raise ValueError("control steps require exactly execution trace v5 or v6")
        if is_control_schema and self.control_evidence is None:
            raise ValueError("execution trace v5/v6 requires root control evidence")
        is_v6 = self.schema_version == "contextunity.execution-trace/v6"
        if is_v6 != (self.final_verdict is not None):
            raise ValueError("FinalVerdict requires exactly execution trace v6")
        if self.final_verdict is not None:
            verdict = self.final_verdict
            if (
                verdict.terminal_status != self.terminal_status
                or verdict.terminal_reason != self.terminal_reason
                or verdict.duration_ms != self.duration_ms
            ):
                raise ValueError("FinalVerdict terminal outcome does not match trace")
            if self.control_evidence is None:
                raise ValueError("FinalVerdict requires root control evidence")
            control = self.control_evidence
            if (
                verdict.node_attempts != control.node_attempts
                or verdict.model_attempts != control.model_attempts
                or verdict.tool_attempts != control.tool_attempts
                or verdict.input_tokens != self.usage.input_tokens
                or verdict.output_tokens != self.usage.output_tokens
                or verdict.cost_micros != self.usage.cost_micros
            ):
                raise ValueError("FinalVerdict totals do not match terminal trace")
            material = {
                "trace_id": str(self.trace_id),
                "graph_run_id": str(self.graph_run_id),
                "terminal_status": verdict.terminal_status,
                "terminal_reason": verdict.terminal_reason,
                "verifier_ref": verdict.verifier_ref,
                "verifier_evidence_refs": verdict.verifier_evidence_refs,
                "fault_class": verdict.fault_class,
                "attribution_candidates": [str(item) for item in verdict.attribution_candidates],
                "node_attempts": verdict.node_attempts,
                "model_attempts": verdict.model_attempts,
                "tool_attempts": verdict.tool_attempts,
                "input_tokens": verdict.input_tokens,
                "output_tokens": verdict.output_tokens,
                "cost_micros": verdict.cost_micros,
                "duration_ms": verdict.duration_ms,
            }
            computed = hashlib.sha256(
                json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if verdict.verdict_digest != computed:
                raise ValueError("FinalVerdict digest mismatch")
        replan_steps = [
            (step, request) for step in self.steps if (request := step.replan_request) is not None
        ]
        if any(request.run_id != self.graph_run_id for _, request in replan_steps):
            raise ValueError("replan request run identity does not match trace")
        is_replan_terminal = self.terminal_reason == "replan_requested"
        if is_replan_terminal != (len(replan_steps) == 1):
            raise ValueError("replan terminal requires exactly one replan request")
        if is_replan_terminal and self.terminal_status != "failed":
            raise ValueError("replan terminal must close the current run as failed")
        node_names = {step.name for step in self.steps if step.kind == "node"}
        for step, request in replan_steps:
            if (
                request.plan_id,
                request.plan_revision,
                request.parent_plan_id,
                request.parent_plan_revision,
                request.prior_replan_ref,
            ) != (
                self.plan_id,
                self.plan_revision,
                self.parent_plan_id,
                self.parent_plan_revision,
                self.replan_ref,
            ):
                raise ValueError("replan request plan lineage does not match trace")
            if not set((*request.failed_task_ids, *request.stalled_task_ids)).issubset(node_names):
                raise ValueError("replan task ids must identify trace node steps")
            control = self.control_evidence
            if control is None:
                raise ValueError("replan request requires root control evidence")
            if not set(request.fault_refs).issubset(control.fault_refs):
                raise ValueError("replan fault refs must be present in control evidence")
            if not set(request.effect_receipt_refs).issubset(control.effect_receipt_refs):
                raise ValueError("replan effect refs must be present in control evidence")
            required_refs = {
                f"policy:{request.policy_digest}",
                f"verifier:{request.verifier_ref}",
                *(f"fault:{item}" for item in request.fault_refs),
                *(f"effect:{item}" for item in request.effect_receipt_refs),
            }
            if not required_refs.issubset(step.evidence_refs):
                raise ValueError("replan request refs must be present on the control step")
        if is_control_schema:
            control = self.control_evidence
            if control is None:
                raise ValueError("execution trace v5 requires root control evidence")
            root_fault_refs = {f"fault:{item}" for item in control.fault_refs}
            root_effect_refs = {f"effect:{item}" for item in control.effect_receipt_refs}
            for step in self.steps:
                fault_refs = {ref for ref in step.evidence_refs if ref.startswith("fault:")}
                effect_refs = {ref for ref in step.evidence_refs if ref.startswith("effect:")}
                if not fault_refs.issubset(root_fault_refs):
                    raise ValueError("control step fault refs must be present in control evidence")
                if not effect_refs.issubset(root_effect_refs):
                    raise ValueError("control step effect refs must be present in control evidence")
        for step in self.steps:
            ref = step.artifact_ref
            if ref is None:
                continue
            identity = ref.identity
            if (
                identity.tenant_id != self.tenant_id
                or identity.project_id != self.project_id
                or identity.trace_id != self.trace_id
                or identity.graph_run_id != self.graph_run_id
            ):
                raise ValueError("protected artifact ref does not match terminal trace identity")
        if self.schema_version == "contextunity.execution-trace/v1":
            consistent = (
                self.terminal_status == "succeeded"
                and self.terminal_reason == "completed"
                or self.terminal_status == "cancelled"
                and self.terminal_reason == "cancelled"
                or self.terminal_status == "failed"
                and self.terminal_reason
                in ("execution_error", "budget_exhausted", "security_rejected")
            )
        else:
            consistent = (
                self.terminal_status == "succeeded"
                and self.terminal_reason == "verified_success"
                or self.terminal_status == "cancelled"
                and self.terminal_reason == "cancelled"
                or self.terminal_status == "failed"
                and self.terminal_reason
                in (
                    "failed",
                    "blocked",
                    "budget_exhausted",
                    "human_review_required",
                    "replan_requested",
                )
            )
        if not consistent:
            raise ValueError("terminal status and reason are inconsistent")
        control = self.control_evidence
        if control is not None and any(
            decision.run_id != self.graph_run_id for decision in control.chronos_decisions
        ):
            raise ValueError("RouterChronos decision run does not match terminal trace")
        return self


class LogTracePayload(StrictPayloadModel):
    """Payload for LogTrace RPC."""

    tenant_id: str | None = None
    agent_id: str | None = None
    session_id: str | None = None
    user_id: str | None = None
    graph_name: str | None = None
    tool_calls: list[JsonDict] = Field(default_factory=list)
    token_usage: JsonDict = Field(default_factory=dict)
    timing_ms: int | None = None
    security_flags: JsonDict = Field(default_factory=dict)
    metadata: JsonDict = Field(default_factory=dict)
    provenance: list[str] = Field(default_factory=list)
    terminal_trace: TerminalExecutionTracePayload | None = None

    @model_validator(mode="after")
    def _select_trace_contract(self) -> "LogTracePayload":
        if self.terminal_trace is None:
            if not self.tenant_id or not self.agent_id:
                raise ValueError("legacy trace requires tenant_id and agent_id")
            return self
        legacy_values = (
            self.tenant_id,
            self.agent_id,
            self.session_id,
            self.user_id,
            self.graph_name,
            self.tool_calls,
            self.token_usage,
            self.timing_ms,
            self.security_flags,
            self.metadata,
            self.provenance,
        )
        if any(value not in (None, "", [], {}) for value in legacy_values):
            raise ValueError("terminal trace cannot include legacy open-content fields")
        return self


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


class PruneExpiredBlackboardPayload(StrictPayloadModel):
    """Payload for PruneExpiredBlackboard RPC."""

    tenant_id: str = Field(min_length=1)


# =====================================================
# Gardener / Human-in-the-Loop
# =====================================================


_MATCH_DUCKDB_S3_HOST = re.compile(
    r"^(?:[a-z0-9][a-z0-9.-]*\.)?s3"
    r"(?:[.-][a-z0-9-]+|\.dualstack\.[a-z0-9-]+|-accelerate(?:\.dualstack)?)?"
    r"\.amazonaws\.com$"
)
_MATCH_DUCKDB_R2_HOST = re.compile(r"^[0-9a-f]{32}\.r2\.cloudflarestorage\.com$")
_MATCH_DUCKDB_SIGNATURE = re.compile(r"^[0-9a-fA-F]{64}$")
_MATCH_DUCKDB_CREDENTIAL = re.compile(r"^[^/]+/\d{8}/[^/]+/s3/aws4_request$")


def _is_match_duckdb_object_host(hostname: str) -> bool:
    """Accept only S3 virtual/path-style and Cloudflare R2 object endpoints."""
    return bool(
        _MATCH_DUCKDB_S3_HOST.fullmatch(hostname) or _MATCH_DUCKDB_R2_HOST.fullmatch(hostname)
    )


class MatchDuckDBPayload(StrictPayloadModel):
    """Payload for MatchDuckDB RPC (HTTPS, SigV4-presigned object URLs only)."""

    tenant_id: str
    unmatched_url: str
    canonical_url: str
    leftovers_put_url: str

    @field_validator("unmatched_url", "canonical_url", "leftovers_put_url")
    @classmethod
    def _require_presigned_object_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError:
            address = None
        hostname = (parsed.hostname or "").lower()
        required = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Date": None,
            "X-Amz-Expires": None,
            "X-Amz-SignedHeaders": None,
            "X-Amz-Credential": None,
            "X-Amz-Signature": None,
        }
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in (None, 443)
            or parsed.fragment
            or parsed.path in ("", "/")
            or not hostname
            or not _is_match_duckdb_object_host(hostname)
            or (address is not None and not address.is_global)
            or any(len(query.get(key, [])) != 1 for key in required)
            or query["X-Amz-Algorithm"][0] != "AWS4-HMAC-SHA256"
            or not re.fullmatch(r"\d{8}T\d{6}Z", query["X-Amz-Date"][0])
            or not query["X-Amz-Expires"][0].isdigit()
            or not 1 <= int(query["X-Amz-Expires"][0]) <= 604800
            or "host"
            not in {item.strip().lower() for item in query["X-Amz-SignedHeaders"][0].split(";")}
            or not _MATCH_DUCKDB_CREDENTIAL.fullmatch(query["X-Amz-Credential"][0])
            or query["X-Amz-Credential"][0].split("/")[1] != query["X-Amz-Date"][0][:8]
            or not _MATCH_DUCKDB_SIGNATURE.fullmatch(query["X-Amz-Signature"][0])
        ):
            raise ValueError("MatchDuckDB URLs must be HTTPS SigV4 presigned object URLs")
        return value

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
