"""Payloads for Brain knowledge and canonical cell operations."""

from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.types import JsonDict
from pydantic import Field, field_validator

# =====================================================
# Core Knowledge Operations
# =====================================================


class SearchCellsPayload(StrictPayloadModel):
    """Closed semantic/hybrid BrainCell search request."""

    tenant_id: str | None = Field(default=None, min_length=1, max_length=128)
    user_id: str | None = Field(default=None, min_length=1, max_length=256)
    query_text: str = Field(min_length=1, max_length=8_192)
    limit: int = Field(default=10, ge=1, le=100)
    min_score: float = Field(default=0.0, ge=0.0, le=1.0)
    source_types: list[str] = Field(default_factory=list, max_length=32)
    scope_path: str | None = Field(default=None, min_length=1, max_length=512)
    metadata_filter: dict[str, str] = Field(default_factory=dict, max_length=2)

    @field_validator("metadata_filter")
    @classmethod
    def validate_metadata_filter(cls, value: dict[str, str]) -> dict[str, str]:
        if any(key not in {"service", "doc_type"} for key in value) or any(
            not item or len(item) > 256 for item in value.values()
        ):
            raise ValueError("metadata_filter accepts bounded service/doc_type values only")
        return dict(sorted(value.items()))


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


class IngestDocumentPayload(StrictPayloadModel):
    """Closed document-ingestion request preserving enrichment semantics."""

    tenant_id: str | None = Field(default=None, min_length=1, max_length=128)
    user_id: str | None = Field(default=None, min_length=1, max_length=256)
    content: str = Field(min_length=1, max_length=1_000_000)
    source_type: str = Field(min_length=1, max_length=128)
    metadata: JsonDict = Field(default_factory=dict)


# =====================================================
# Phase 3: Canonical BrainCell payloads (M01)
# Per planner/roadmap/phase-03/tasks/braincell-canonical-api.md
# =====================================================


class UpsertCellPayload(StrictPayloadModel):
    """Payload for UpsertCell RPC (canonical over cells table)."""

    tenant_id: str | None = None
    cell_id: str | None = None
    user_id: str | None = None
    cell_kind: str
    content: str
    metadata: JsonDict = Field(default_factory=dict)
    scope_path: str | None = None
    content_hash: str | None = None
    source_type: str = "manual"
    source_ref: str | None = None
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    visibility: str = "tenant"


class QueryCellsPayload(StrictPayloadModel):
    """Payload for QueryCells RPC."""

    tenant_id: str | None = None
    user_id: str | None = None
    query_text: str | None = None
    cell_kind: str | None = None
    source_type: str | None = None
    scope_path: str | None = None
    metadata_filter: JsonDict = Field(default_factory=dict)
    limit: int = Field(default=10, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=1_000_000)


class GetCellPayload(StrictPayloadModel):
    """Payload for GetCell RPC."""

    tenant_id: str | None = None
    user_id: str | None = None
    cell_id: str


class DocumentationDeleteTarget(StrictPayloadModel):
    """One exact documentation cell/version target for atomic deletion."""

    cell_id: str = Field(min_length=1, max_length=128)
    content_hash: str = Field(min_length=1, max_length=256)


class DeleteDocumentationCellsPayload(StrictPayloadModel):
    """Payload for atomic, exact documentation-cell deletion."""

    tenant_id: str | None = None
    targets: list[DocumentationDeleteTarget] = Field(min_length=1, max_length=1_000)
