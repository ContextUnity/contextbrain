"""Brain-local application port for UniversalDebugBus lifecycle operations."""

from __future__ import annotations

from uuid import UUID

from contextunity.core.udb import (
    DebugCase,
    DebugCaseDetail,
    DebugCaseQuery,
    FaultOccurrence,
    MitigationAttempt,
    RecoveryEvidence,
    ReopenDebugCase,
    ResolveDebugCase,
)

from contextunity.brain.core.exceptions import UdbFeatureDisabledError
from contextunity.brain.storage.protocols.udb import UdbStorageProtocol


class UdbApplication:
    """Gate-owning application service shared by gRPC and Brain-local callers."""

    def __init__(self, *, storage: UdbStorageProtocol, enabled: bool) -> None:
        self._storage = storage
        self._enabled = enabled

    def _require_enabled(self) -> None:
        if not self._enabled:
            raise UdbFeatureDisabledError()

    async def report_fault_occurrence(self, occurrence: FaultOccurrence) -> DebugCase:
        self._require_enabled()
        return await self._storage.report_fault_occurrence(occurrence)

    async def report_recovery_evidence(
        self, *, tenant_id: str, evidence: RecoveryEvidence
    ) -> DebugCase:
        self._require_enabled()
        return await self._storage.report_recovery_evidence(
            tenant_id=tenant_id,
            evidence=evidence,
        )

    async def report_mitigation_attempt(
        self, *, tenant_id: str, attempt: MitigationAttempt
    ) -> DebugCase:
        self._require_enabled()
        return await self._storage.report_mitigation_attempt(
            tenant_id=tenant_id,
            attempt=attempt,
        )

    async def resolve_debug_case(self, *, tenant_id: str, command: ResolveDebugCase) -> DebugCase:
        self._require_enabled()
        return await self._storage.resolve_debug_case(tenant_id=tenant_id, command=command)

    async def reopen_debug_case(self, *, tenant_id: str, command: ReopenDebugCase) -> DebugCase:
        self._require_enabled()
        return await self._storage.reopen_debug_case(tenant_id=tenant_id, command=command)

    async def get_debug_case(self, *, tenant_id: str, case_id: UUID) -> DebugCase | None:
        self._require_enabled()
        return await self._storage.get_debug_case(tenant_id=tenant_id, case_id=case_id)

    async def get_debug_case_detail(
        self,
        *,
        tenant_id: str,
        case_id: UUID,
        history_limit: int,
    ) -> DebugCaseDetail | None:
        self._require_enabled()
        return await self._storage.get_debug_case_detail(
            tenant_id=tenant_id,
            case_id=case_id,
            history_limit=history_limit,
        )

    async def query_debug_cases(
        self,
        *,
        tenant_id: str,
        query: DebugCaseQuery,
        recurring_only: bool = False,
    ) -> list[DebugCase]:
        self._require_enabled()
        return await self._storage.query_debug_cases(
            tenant_id=tenant_id,
            query=query,
            recurring_only=recurring_only,
        )


__all__ = ["UdbApplication"]
