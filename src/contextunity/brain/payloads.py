"""Pydantic payload models for Brain gRPC operations.

These models provide server-side validation for ContextUnit payloads.
Each RPC method extracts and validates its payload using these models.

Example usage in service:
    from .payloads import SearchPayload

    async def Search(self, request, context):
        unit = ContextUnit.from_protobuf(request)
        params = SearchPayload.model_validate(unit.payload or {})
        # params.tenant_id, params.query_text, etc.
"""

from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.types import JsonDict, JsonValue, is_object_dict
from pydantic import Field, model_validator

# =====================================================
# Core Knowledge Operations
# =====================================================


class SearchPayload(StrictPayloadModel):
    """Payload for Search RPC."""

    tenant_id: str
    user_id: str | None = None
    query_text: str
    limit: int = 10
    min_score: float = 0.0
    source_types: list[str] = Field(default_factory=list)


class GraphSearchPayload(StrictPayloadModel):
    """Payload for GraphSearch RPC.

    Structural graph traversal starting from known entity IDs,
    walking edges up to max_hops.
    """

    tenant_id: str
    user_id: str | None = None
    entrypoint_ids: list[str]
    max_hops: int = Field(default=2, ge=1, le=10)
    allowed_relations: list[str] = Field(default_factory=list)
    max_results: int = Field(default=200, ge=1, le=1000)


class CreateKGRelationPayload(StrictPayloadModel):
    """Payload for CreateKGRelation RPC."""

    tenant_id: str
    user_id: str | None = None
    source_type: str
    source_id: str
    relation: str
    target_type: str
    target_id: str


class UpsertPayload(StrictPayloadModel):
    """Payload for Upsert RPC."""

    tenant_id: str
    user_id: str | None = None
    content: str
    source_type: str
    metadata: JsonDict = Field(default_factory=dict)


class QueryMemoryPayload(StrictPayloadModel):
    """Payload for QueryMemory RPC."""

    tenant_id: str
    user_id: str | None = None
    content: str
    filters: JsonDict = Field(default_factory=dict)


# Episodic & Entity Memory
# =====================================================


class AddEpisodePayload(StrictPayloadModel):
    """Payload for AddEpisode RPC."""

    tenant_id: str
    user_id: str | None = None
    session_id: str | None = None
    content: str
    metadata: JsonDict = Field(default_factory=dict)


class UpsertFactPayload(StrictPayloadModel):
    """Payload for UpsertFact RPC."""

    user_id: str
    tenant_id: str
    key: str
    value: JsonValue
    confidence: float = 1.0
    source_id: str | None = None


class GetRecentEpisodesPayload(StrictPayloadModel):
    """Payload for GetRecentEpisodes RPC."""

    tenant_id: str
    user_id: str
    limit: int = 5


class GetUserFactsPayload(StrictPayloadModel):
    """Payload for GetUserFacts RPC."""

    tenant_id: str
    user_id: str


class RetentionCleanupPayload(StrictPayloadModel):
    """Payload for RetentionCleanup RPC."""

    tenant_id: str
    older_than_days: int = 30
    episode_ids: list[str] | None = None


class GetEpisodeStatsPayload(StrictPayloadModel):
    """Payload for GetEpisodeStats RPC."""

    tenant_id: str


# =====================================================
# Agent Traces
# =====================================================


class LogTracePayload(StrictPayloadModel):
    """Payload for LogTrace RPC."""

    tenant_id: str
    agent_id: str
    session_id: str | None = None
    user_id: str | None = None
    graph_name: str | None = None
    tool_calls: list[JsonDict] = Field(default_factory=list)
    token_usage: JsonDict = Field(default_factory=dict)
    timing_ms: int | None = None
    security_flags: JsonDict = Field(default_factory=dict)
    metadata: JsonDict = Field(default_factory=dict)
    provenance: list[str] = Field(default_factory=list)


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


class UpsertTaxonomyPayload(StrictPayloadModel):
    """Payload for UpsertTaxonomy RPC."""

    tenant_id: str
    domain: str = "general"
    name: str
    path: str | None = None
    keywords: list[str] = Field(default_factory=list)
    metadata: JsonDict = Field(default_factory=dict)


class GetTaxonomyPayload(StrictPayloadModel):
    """Payload for GetTaxonomy RPC."""

    tenant_id: str
    domain: str | None = None


# =====================================================
# Blackboard Operations (Flat Memory)
# =====================================================


class WriteBlackboardPayload(StrictPayloadModel):
    """Payload for WriteBlackboard RPC."""

    tenant_id: str
    scope_path: str  # LTREE path: 'tenant.project.session.step'
    content: JsonDict
    metadata: JsonDict = Field(default_factory=dict)
    ttl_seconds: int | None = None
    created_by: str | None = None


class ReadBlackboardPayload(StrictPayloadModel):
    """Payload for ReadBlackboard RPC."""

    ids: list[str]  # UUID strings


# =====================================================
# Gardener / Human-in-the-Loop
# =====================================================


class GetPendingPayload(StrictPayloadModel):
    """Payload for GetPendingVerifications RPC."""

    tenant_id: str
    limit: int = 50


class SubmitVerificationPayload(StrictPayloadModel):
    """Payload for SubmitVerification RPC."""

    id: str
    enrichment_json: str


class MatchDuckDBPayload(StrictPayloadModel):
    """Payload for MatchDuckDB RPC (presigned Parquet URLs)."""

    tenant_id: str
    unmatched_url: str
    canonical_url: str
    leftovers_put_url: str

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


# =====================================================
# Experience Memory (Flat Memory Phase B)
# =====================================================


class RecordExperiencePayload(StrictPayloadModel):
    """Payload for RecordExperience RPC."""

    tenant_id: str
    agent_id: str
    action_type: str
    action_data: JsonDict
    graph_name: str | None = None
    graph_run_id: str | None = None
    context_summary: str | None = None
    client_id: str | None = None
    node_role: str = "worker"
    scope_path: str | None = None
    q_action: float = 0.5
    q_hypothesis: float = 0.5
    q_relevance: float = 0.5


class QueryExperiencesPayload(StrictPayloadModel):
    """Payload for QueryExperiences RPC."""

    tenant_id: str
    action_type: str | None = None
    context_embedding: list[float] | None = None
    min_q: float = Field(default=0.6, ge=0.0, le=1.0)
    limit: int = Field(default=5, ge=1, le=50)
    exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    agent_id: str | None = None
    scope_path: str | None = None


class UpdateExperienceQPayload(StrictPayloadModel):
    """Payload for UpdateExperienceQ RPC."""

    experience_id: str
    q_action: float | None = None
    q_hypothesis: float | None = None
    q_relevance: float | None = None
    fault_class: str | None = None
    status: str | None = None
