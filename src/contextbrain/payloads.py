"""Pydantic payload models for Brain gRPC operations.

These models provide server-side validation for ContextUnit payloads.
Each RPC method extracts and validates its payload using these models.

Example usage in service:
    from .payloads import SearchPayload

    async def Search(self, request, context):
        unit = ContextUnit.from_protobuf(request)
        params = SearchPayload(**unit.payload)
        # params.tenant_id, params.query_text, etc.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field

# =====================================================
# Core Knowledge Operations
# =====================================================


class SearchPayload(BaseModel):
    """Payload for Search RPC."""

    tenant_id: str
    user_id: Optional[str] = None
    query_text: str
    limit: int = 10
    min_score: float = 0.0
    source_types: list[str] = Field(default_factory=list)


class GraphSearchPayload(BaseModel):
    """Payload for GraphSearch RPC.

    Structural graph traversal starting from known entity IDs,
    walking edges up to max_hops.
    """

    tenant_id: str
    user_id: Optional[str] = None
    entrypoint_ids: list[str]
    max_hops: int = Field(default=2, ge=1, le=10)
    allowed_relations: list[str] = Field(default_factory=list)
    max_results: int = Field(default=200, ge=1, le=1000)


class CreateKGRelationPayload(BaseModel):
    """Payload for CreateKGRelation RPC."""

    tenant_id: str
    user_id: Optional[str] = None
    source_type: str
    source_id: str
    relation: str
    target_type: str
    target_id: str


class UpsertPayload(BaseModel):
    """Payload for Upsert RPC."""

    tenant_id: str
    user_id: Optional[str] = None
    content: str
    source_type: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class QueryMemoryPayload(BaseModel):
    """Payload for QueryMemory RPC."""

    tenant_id: str = "default"
    user_id: Optional[str] = None
    content: str
    filters: dict[str, Any] = Field(default_factory=dict)


# =====================================================
# NewsEngine Operations
# =====================================================


class UpsertNewsItemPayload(BaseModel):
    """Payload for UpsertNewsItem RPC."""

    tenant_id: str
    item_type: str = "raw"  # "raw" or "fact"
    url: str
    headline: str
    summary: str = ""
    category: str = ""
    source_api: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    harvested_at: Optional[str] = None


class GetNewsItemsPayload(BaseModel):
    """Payload for GetNewsItems RPC."""

    tenant_id: str
    item_type: str = "fact"  # "raw" or "fact"
    limit: int = 20
    since: Optional[str] = None  # ISO datetime


class UpsertNewsPostPayload(BaseModel):
    """Payload for UpsertNewsPost RPC."""

    tenant_id: str
    headline: str
    content: str
    agent: str
    emoji: str = "📰"
    fact_url: str = ""
    fact_id: str = ""
    scheduled_at: Optional[str] = None


class CheckNewsPostExistsPayload(BaseModel):
    """Payload for CheckNewsPostExists RPC."""

    tenant_id: str
    fact_url: str


# =====================================================
# Episodic & Entity Memory
# =====================================================


class AddEpisodePayload(BaseModel):
    """Payload for AddEpisode RPC."""

    user_id: str = "anonymous"
    tenant_id: str = "default"
    session_id: Optional[str] = None
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class UpsertFactPayload(BaseModel):
    """Payload for UpsertFact RPC."""

    user_id: str
    tenant_id: str = "default"
    key: str
    value: Any
    confidence: float = 1.0
    source_id: Optional[str] = None


class GetRecentEpisodesPayload(BaseModel):
    """Payload for GetRecentEpisodes RPC."""

    tenant_id: str = "default"
    user_id: str
    limit: int = 5


class GetUserFactsPayload(BaseModel):
    """Payload for GetUserFacts RPC."""

    tenant_id: str = "default"
    user_id: str


class RetentionCleanupPayload(BaseModel):
    """Payload for RetentionCleanup RPC."""

    tenant_id: str = "default"
    older_than_days: int = 30
    episode_ids: list[str] | None = None


# =====================================================
# Agent Traces
# =====================================================


class LogTracePayload(BaseModel):
    """Payload for LogTrace RPC."""

    tenant_id: str
    agent_id: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    graph_name: Optional[str] = None
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    token_usage: dict[str, Any] = Field(default_factory=dict)
    timing_ms: Optional[int] = None
    security_flags: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    provenance: list[str] = Field(default_factory=list)


class GetTracesPayload(BaseModel):
    """Payload for GetTraces RPC."""

    tenant_id: str
    agent_id: Optional[str] = None
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    limit: int = 20
    since: Optional[str] = None  # ISO datetime


# =====================================================
# Taxonomy Operations
# =====================================================


class UpsertTaxonomyPayload(BaseModel):
    """Payload for UpsertTaxonomy RPC."""

    tenant_id: str = "default"
    domain: str = "general"
    name: str
    path: Optional[str] = None
    keywords: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GetTaxonomyPayload(BaseModel):
    """Payload for GetTaxonomy RPC."""

    tenant_id: str = "default"
    domain: Optional[str] = None


# =====================================================
# Commerce/Dealer Operations
# =====================================================


class GetProductsPayload(BaseModel):
    """Payload for GetProducts RPC."""

    tenant_id: str
    product_ids: list[int]


class UpsertDealerProductPayload(BaseModel):
    """Payload for UpsertDealerProduct RPC."""

    tenant_id: str
    dealer_code: str
    dealer_name: str
    sku: str
    name: str = ""
    category: str = ""
    brand_name: str = ""
    quantity: int = 0
    price_retail: float = 0.0
    currency: str = "UAH"
    params: dict[str, Any] = Field(default_factory=dict)
    status: str = "raw"  # raw, enriched, pending_human
    trace_id: Optional[str] = None


class UpdateEnrichmentPayload(BaseModel):
    """Payload for UpdateEnrichment RPC."""

    tenant_id: str
    product_id: int
    enrichment: dict[str, Any]
    trace_id: Optional[str] = None
    status: str = "enriched"


# =====================================================
# Gardener / Human-in-the-Loop
# =====================================================


class GetPendingPayload(BaseModel):
    """Payload for GetPendingVerifications RPC."""

    tenant_id: str
    limit: int = 50


class SubmitVerificationPayload(BaseModel):
    """Payload for SubmitVerification RPC."""

    id: str
    enrichment_json: str
