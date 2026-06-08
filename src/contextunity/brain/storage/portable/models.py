"""Pydantic models for Portable Archive v1 records.
All models use ``ConfigDict(extra="forbid")`` — malformed records
are rejected at parse time, never silently imported.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Literal

from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, JsonValue, is_json_dict
from pydantic import BaseModel, ConfigDict, Field

from contextunity.brain.core.exceptions import BrainValidationError

ARCHIVE_FORMAT = "contextunity.brain.portable.v1"

# ── Manifest ──────────────────────────────────────────────────────


class PortableManifest(BaseModel):
    """Archive manifest — validated before any import."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    format: Literal["contextunity.brain.portable.v1"] = ARCHIVE_FORMAT
    source_backend: str = "sqlite-vec"
    schema_version: int = 1
    vector_dim: int = 1536
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


class KnowledgeNodeRecord(BaseModel):
    """Represent and manage Knowledge Node Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["knowledge_node"] = "knowledge_node"
    tenant_id: str
    id: str
    content: str
    node_kind: str = "concept"
    source_type: str | None = None
    source_id: str | None = None
    title: str | None = None
    keywords_text: str | None = None
    taxonomy_path: str | None = None
    metadata: JsonDict = Field(default_factory=dict)
    user_id: str | None = None
    embedding_ref: str | None = None


class KnowledgeEdgeRecord(BaseModel):
    """Represent and manage Knowledge Edge Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["knowledge_edge"] = "knowledge_edge"
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


class FactRecord(BaseModel):
    """Represent and manage Fact Record logic within the system."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")
    type: Literal["fact"] = "fact"
    tenant_id: str
    user_id: str
    fact_key: str
    fact_value: JsonValue
    confidence: float = 1.0
    source_id: str | None = None


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
    "knowledge_node": KnowledgeNodeRecord,
    "knowledge_edge": KnowledgeEdgeRecord,
    "episode": EpisodeRecord,
    "fact": FactRecord,
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
    "ARCHIVE_FORMAT",
    "PortableManifest",
    "BlackboardRecord",
    "TraceRecord",
    "TaxonomyRecord",
    "KnowledgeNodeRecord",
    "KnowledgeEdgeRecord",
    "EpisodeRecord",
    "FactRecord",
    "EmbeddingRecord",
    "RECORD_TYPES",
    "parse_record",
]
