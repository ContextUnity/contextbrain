"""Pydantic models for the current Portable Archive records.
All models use ``ConfigDict(extra="forbid")`` — malformed records
are rejected at parse time, never silently imported.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Literal

from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, is_json_dict
from pydantic import BaseModel, ConfigDict, Field, model_validator

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION

# ── Manifest ──────────────────────────────────────────────────────


class PortableManifest(BaseModel):
    """Archive manifest — validated before any import."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    source_backend: str = "sqlite-vec"
    vector_dim: int = DEFAULT_EMBEDDING_DIMENSION
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    record_counts: dict[str, int] = Field(default_factory=dict)
    tenants: list[str] = Field(default_factory=list)


# ── Record types ──────────────────────────────────────────────────


class BlackboardRecord(BaseModel):
    """Represent and manage Blackboard Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["blackboard"] = "blackboard"
    tenant_id: str
    id: str
    scope_path: str
    content: JsonDict
    metadata: JsonDict = Field(default_factory=dict)
    created_by: str | None = None
    created_at: str
    ttl_until: str | None = None


class TraceRecord(BaseModel):
    """Represent and manage Trace Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["trace"] = "trace"
    tenant_id: str
    trace_id: str
    agent_id: str
    session_id: str | None = None
    user_id: str | None = None
    graph_name: str | None = None
    tool_calls: list[JsonDict] = Field(default_factory=list)
    token_usage: JsonDict = Field(default_factory=dict)
    timing_ms: int | None = None
    metadata: JsonDict = Field(default_factory=dict)
    security_flags: JsonDict = Field(default_factory=dict)
    provenance: list[str] | None = None
    graph_run_id: str | None = None
    payload_digest: str | None = None
    terminal_status: str | None = None
    terminal_reason: str | None = None
    trace_schema_version: str = "legacy_v0"
    prompt_evidence: list[JsonDict] = Field(default_factory=list, max_length=64)
    steps: list[JsonDict] = Field(default_factory=list, max_length=4096)
    control_evidence: JsonDict = Field(default_factory=dict)
    final_verdict: JsonDict = Field(default_factory=dict)
    created_at: str

    @model_validator(mode="after")
    def validate_closed_execution_trace_steps(self) -> "TraceRecord":
        """Canonicalize closed v3+ steps before a portable backend persists them."""
        if self.trace_schema_version not in {
            "contextunity.execution-trace/v3",
            "contextunity.execution-trace/v4",
            "contextunity.execution-trace/v5",
            "contextunity.execution-trace/v6",
        }:
            return self
        if (
            self.trace_schema_version
            in {"contextunity.execution-trace/v5", "contextunity.execution-trace/v6"}
            and not self.control_evidence
        ):
            raise ValueError("portable execution trace v5/v6 requires root control evidence")
        if (self.trace_schema_version == "contextunity.execution-trace/v6") != bool(
            self.final_verdict
        ):
            raise ValueError("portable FinalVerdict requires exactly execution trace v6")
        from contextunity.brain.payloads.memory import (
            TraceStepPayload,
            validate_guidance_step_placement,
        )

        parsed_steps = [TraceStepPayload.model_validate(step) for step in self.steps]
        validate_guidance_step_placement(parsed_steps)
        canonical_steps: list[JsonDict] = []
        for step in parsed_steps:
            dumped = step.model_dump(mode="json", exclude_none=True)
            if not is_json_dict(dumped):
                raise ValueError("portable execution-trace step must be a JSON object")
            canonical_steps.append(dumped)
        self.steps = canonical_steps
        return self


class CellRecord(BaseModel):
    """Canonical BrainCell archive record."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["cell"] = "cell"
    tenant_id: str
    id: str
    content: str
    cell_kind: str = "concept"
    source_type: str = "manual"
    source_ref: str | None = None
    scope_path: str | None = None
    content_hash: str
    confidence: float = Field(ge=0.0, le=1.0)
    visibility: str = "tenant"
    metadata: JsonDict = Field(default_factory=dict)
    user_id: str | None = None
    created_at: str
    updated_at: str
    embedding_ref: str | None = None


class CellEdgeRecord(BaseModel):
    """Represent and manage CellEdge Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["cell_edge"] = "cell_edge"
    tenant_id: str
    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0
    metadata: JsonDict = Field(default_factory=dict)


class ConversationArchiveRecord(BaseModel):
    """Canonical Conversation History portable record."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["conversation"] = "conversation"
    tenant_id: str
    record_id: str
    user_id: str
    session_id: str | None = None
    role: Literal["user", "assistant", "system", "tool", "legacy"]
    kind: Literal["message", "turn_summary", "conversation_note", "legacy_import"]
    content: str
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    source_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    graph_run_id: str | None = None
    metadata_version: Literal[1] = 1
    idempotency_key: str
    metadata: JsonDict = Field(default_factory=dict)
    created_at: str


class OutcomeObservationArchiveRecord(BaseModel):
    """Immutable delayed outcome and its original Brain resolution receipt."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["outcome_observation"] = "outcome_observation"
    observation_id: str
    tenant_id: str
    trace_id: str
    graph_run_id: str
    verdict_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_kind: Literal["verified_success", "verified_failure", "neutral"]
    source_authority: Literal["operator_review/v1"]
    source_ref: str
    occurred_at: str
    idempotency_key: str
    canonical_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_version: str
    resolution_receipt: JsonDict
    created_at: str


class SynapseRecord(BaseModel):
    """Represent and manage BrainSynapse archive records."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["synapse"] = "synapse"
    tenant_id: str
    id: str
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
    q_action: float = 0.5
    q_hypothesis: float = 0.5
    q_relevance: float = 0.5
    q_composite: float = 0.5
    metadata: JsonDict = Field(default_factory=dict)
    created_at: str
    updated_at: str


class EmbeddingRecord(BaseModel):
    """Represent and manage Embedding Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    ref: str  # e.g. "emb:node:abc123"
    vector: list[float]


# Type discriminator map
RECORD_TYPES: dict[str, type[BaseModel]] = {
    "blackboard": BlackboardRecord,
    "trace": TraceRecord,
    "cell": CellRecord,
    "cell_edge": CellEdgeRecord,
    "conversation": ConversationArchiveRecord,
    "outcome_observation": OutcomeObservationArchiveRecord,
    "synapse": SynapseRecord,
}


def parse_record(line: str) -> BaseModel:
    """Parse a JSONL line into the appropriate record model.

    Args:
        line (str): The line parameter.

    Returns:
        BaseModel: An instance of BaseModel.

    Raises:
        ValueError: If parameter values are invalid.
    """
    decoded = parse_wire_json(line)
    if not is_json_dict(decoded):
        raise BrainValidationError("Record line must be a JSON object")
    record_type = decoded.get("type")
    if not isinstance(record_type, str) or record_type not in RECORD_TYPES:
        raise BrainValidationError(f"Unknown record type: {record_type!r}")
    return RECORD_TYPES[record_type].model_validate(decoded)


__all__ = [
    "PortableManifest",
    "BlackboardRecord",
    "TraceRecord",
    "CellRecord",
    "CellEdgeRecord",
    "ConversationArchiveRecord",
    "OutcomeObservationArchiveRecord",
    "SynapseRecord",
    "EmbeddingRecord",
    "RECORD_TYPES",
    "parse_record",
]
