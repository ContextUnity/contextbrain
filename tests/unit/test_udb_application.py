"""Tests for the shared Brain-local UDB application port and C0 gate."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from contextunity.core.udb import FaultOccurrence, UdbComparisonKey

from contextunity.brain.core.exceptions import UdbFeatureDisabledError
from contextunity.brain.storage.sqlite import SqliteBrainStore
from contextunity.brain.udb_application import UdbApplication


def _occurrence() -> FaultOccurrence:
    return FaultOccurrence(
        occurrence_id=uuid4(),
        tenant_id="acme",
        producer_id="router:test",
        idempotency_key="run:1",
        fingerprint_version="contextunity.udb-fingerprint/v1",
        fingerprint="a" * 64,
        fault_class="upstream_fault",
        operation_kind="brain_search",
        fault_code="brain.search.unavailable",
        comparison_key=UdbComparisonKey(
            tenant_id="acme",
            operation_kind="brain_search",
            capability_class="brain:search",
        ),
        occurred_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_udb_application_gate_off_rejects_without_storage_write(tmp_path: Path) -> None:
    """The product default is fail-closed and creates no occurrence row."""
    store = SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    app = UdbApplication(storage=store, enabled=False)

    with pytest.raises(UdbFeatureDisabledError):
        await app.report_fault_occurrence(_occurrence())

    with store.get_sqlite_connection() as db:
        count = db.execute("SELECT COUNT(*) FROM debug_case_occurrences").fetchone()
    assert count is not None and count[0] == 0


def test_all_declared_udb_rpcs_are_live_and_permission_mapped() -> None:
    from contextunity.brain.service.brain_service import BrainService
    from contextunity.brain.service.interceptors import RPC_PERMISSION_MAP

    expected = {
        "ReportFaultOccurrence",
        "ReportMitigationAttempt",
        "ReportRecoveryEvidence",
        "ResolveDebugCase",
        "ReopenDebugCase",
        "GetDebugCase",
        "QueryDebugCases",
        "QueryRecurringFaults",
    }
    for method in expected:
        assert "handlers.udb" in getattr(BrainService, method).__module__
        assert method in RPC_PERMISSION_MAP


@pytest.mark.asyncio
async def test_udb_application_gate_on_reaches_same_storage_authority(tmp_path: Path) -> None:
    """Remote handlers and Brain-local callers share one application service."""
    store = SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    app = UdbApplication(storage=store, enabled=True)

    case = await app.report_fault_occurrence(_occurrence())
    loaded = await app.get_debug_case(tenant_id="acme", case_id=case.case_id)

    assert loaded == case
