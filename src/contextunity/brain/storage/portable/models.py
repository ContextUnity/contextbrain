"""Pydantic models for the current Portable Archive records.
All models use ``ConfigDict(extra="forbid")`` — malformed records
are rejected at parse time, never silently imported.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Literal

from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, is_json_dict
from pydantic import BaseModel, ConfigDict, Field

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
    provenance: list[str] | None = None
    created_at: str


class TaxonomyRecord(BaseModel):
    """Represent and manage Taxonomy Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["taxonomy"] = "taxonomy"
    tenant_id: str
    domain: str
    name: str
    path: str
    keywords: list[str] = Field(default_factory=list)
    metadata: JsonDict = Field(default_factory=dict)


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


class EpisodeRecord(BaseModel):
    """Represent and manage Episode Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["episode"] = "episode"
    tenant_id: str
    user_id: str
    episode_id: str
    content: str
    session_id: str | None = None
    metadata: JsonDict = Field(default_factory=dict)
    created_at: str
    embedding_ref: str | None = None


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
    "taxonomy": TaxonomyRecord,
    "cell": CellRecord,
    "cell_edge": CellEdgeRecord,
    "episode": EpisodeRecord,
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
    "TaxonomyRecord",
    "CellRecord",
    "CellEdgeRecord",
    "EpisodeRecord",
    "SynapseRecord",
    "EmbeddingRecord",
    "RECORD_TYPES",
    "parse_record",
]
