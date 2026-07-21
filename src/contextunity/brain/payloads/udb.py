"""Closed ContextUnit payloads for UniversalDebugBus RPCs."""

from __future__ import annotations

from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.udb import (
    DebugCaseQuery,
    FaultOccurrence,
    MitigationAttempt,
    RecoveryEvidence,
    ReopenDebugCase,
    ResolveDebugCase,
)
from pydantic import Field


class ReportFaultOccurrencePayload(StrictPayloadModel):
    occurrence: FaultOccurrence


class ReportRecoveryEvidencePayload(StrictPayloadModel):
    evidence: RecoveryEvidence


class ReportMitigationAttemptPayload(StrictPayloadModel):
    attempt: MitigationAttempt


class ResolveDebugCasePayload(StrictPayloadModel):
    command: ResolveDebugCase


class ReopenDebugCasePayload(StrictPayloadModel):
    command: ReopenDebugCase


class GetDebugCasePayload(StrictPayloadModel):
    """A tenant request is validated against the verified caller token."""

    case_id: str = Field(min_length=1, max_length=64)
    tenant_id: str | None = Field(default=None, min_length=1, max_length=128)
    include_history: bool = False
    history_limit: int = Field(default=20, ge=1, le=100)


class QueryDebugCasesPayload(StrictPayloadModel):
    query: DebugCaseQuery
    tenant_id: str | None = Field(default=None, min_length=1, max_length=128)


__all__ = [
    "GetDebugCasePayload",
    "QueryDebugCasesPayload",
    "ReopenDebugCasePayload",
    "ReportFaultOccurrencePayload",
    "ReportMitigationAttemptPayload",
    "ReportRecoveryEvidencePayload",
    "ResolveDebugCasePayload",
]
