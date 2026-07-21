"""Storage protocol for the Brain-owned UniversalDebugBus aggregate."""

from __future__ import annotations

from typing import Protocol
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


class UdbStorageProtocol(Protocol):
    """Methods consumed by UDB handlers and the Brain-local reporter port."""

    async def report_fault_occurrence(self, occurrence: FaultOccurrence) -> DebugCase:
        """Persist an occurrence and correlate it into its tenant case."""
        ...

    async def report_recovery_evidence(
        self,
        *,
        tenant_id: str,
        evidence: RecoveryEvidence,
    ) -> DebugCase:
        """Persist comparable recovery evidence with case-revision CAS."""
        ...

    async def report_mitigation_attempt(
        self,
        *,
        tenant_id: str,
        attempt: MitigationAttempt,
    ) -> DebugCase:
        """Persist a revision-bound mitigation attempt."""
        ...

    async def resolve_debug_case(
        self,
        *,
        tenant_id: str,
        command: ResolveDebugCase,
    ) -> DebugCase:
        """Resolve a recovered case with revision CAS."""
        ...

    async def reopen_debug_case(
        self,
        *,
        tenant_id: str,
        command: ReopenDebugCase,
    ) -> DebugCase:
        """Reopen a resolved case using a persisted trigger occurrence."""
        ...

    async def get_debug_case(self, *, tenant_id: str, case_id: UUID) -> DebugCase | None:
        """Return one tenant-owned case without widening scope."""
        ...

    async def get_debug_case_detail(
        self,
        *,
        tenant_id: str,
        case_id: UUID,
        history_limit: int,
    ) -> DebugCaseDetail | None:
        """Return one case and independently bounded closed history lists."""
        ...

    async def query_debug_cases(
        self,
        *,
        tenant_id: str,
        query: DebugCaseQuery,
        recurring_only: bool = False,
    ) -> list[DebugCase]:
        """Return bounded tenant-owned cases."""
        ...


__all__ = ["UdbStorageProtocol"]
