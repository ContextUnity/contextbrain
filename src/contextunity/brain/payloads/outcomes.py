"""Closed delayed-outcome contracts owned by ContextBrain."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from contextunity.core.sdk.types import StrictPayloadModel
from pydantic import Field, field_validator


class OutcomeObservationPayload(StrictPayloadModel):
    """Immutable fact submitted by an independently authorized reviewer.

    The contract deliberately has no Synapse identity and no Q-value field.
    Attribution and learning values are resolved by Brain from FinalVerdict.
    """

    trace_id: UUID
    graph_run_id: UUID
    verdict_digest: str = Field(pattern=r"[0-9a-f]{64}")
    observation_kind: Literal["verified_success", "verified_failure", "neutral"]
    source_authority: Literal["operator_review/v1"]
    source_ref: str = Field(pattern=r"review:[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}")
    occurred_at: datetime
    idempotency_key: str = Field(
        min_length=1, max_length=128, pattern=r"[A-Za-z0-9][A-Za-z0-9._:@/-]*"
    )

    @field_validator("occurred_at")
    @classmethod
    def _canonical_occurred_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(timezone.utc)


class ReportOutcomeObservationPayload(StrictPayloadModel):
    observation: OutcomeObservationPayload


__all__ = ["OutcomeObservationPayload", "ReportOutcomeObservationPayload"]
