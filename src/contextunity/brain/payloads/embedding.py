"""Payloads for Brain cell embedding operations."""

from contextunity.core.sdk.types import StrictPayloadModel
from pydantic import Field


class EnqueueCellEmbeddingPayload(StrictPayloadModel):
    """Request to durably enqueue one stored cell for embedding."""

    tenant_id: str = Field(min_length=1, max_length=128)
    cell_id: str = Field(min_length=1, max_length=128)
    content_hash: str = Field(min_length=1, max_length=128)
    profile: str | None = Field(default=None, min_length=1, max_length=128)


class ClaimCellEmbeddingJobsPayload(StrictPayloadModel):
    """Request for a bounded tenant-scoped embedding lease batch."""

    tenant_id: str = Field(min_length=1, max_length=128)
    limit: int = Field(default=10, ge=1, le=1000)


class EmbedClaimedCellPayload(StrictPayloadModel):
    """Request to process one leased embedding job inside Brain."""

    tenant_id: str = Field(min_length=1, max_length=128)
    job_id: str = Field(min_length=1, max_length=128)
    lease_id: str = Field(min_length=1, max_length=128)


class FailCellEmbeddingJobPayload(StrictPayloadModel):
    """Request to mark one leased job as terminally failed."""

    tenant_id: str = Field(min_length=1, max_length=128)
    job_id: str = Field(min_length=1, max_length=128)
    lease_id: str = Field(min_length=1, max_length=128)
    error_code: str = Field(min_length=1, max_length=128)


class GetCellEmbeddingStatusPayload(StrictPayloadModel):
    """Request for status of one cell/profile idempotency key."""

    tenant_id: str = Field(min_length=1, max_length=128)
    cell_id: str = Field(min_length=1, max_length=128)
    content_hash: str | None = Field(default=None, max_length=128)
    profile: str | None = Field(default=None, min_length=1, max_length=128)


class GetEmbeddingCapabilityPayload(StrictPayloadModel):
    """Tenant-scoped request for embedding runtime readiness."""

    tenant_id: str = Field(min_length=1, max_length=128)
