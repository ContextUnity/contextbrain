"""Brain-local UDB reporter coverage: direct app port, gate, and failure containment."""

from __future__ import annotations

from pathlib import Path

import pytest

from contextunity.brain.storage.sqlite import SqliteBrainStore
from contextunity.brain.udb_application import UdbApplication
from contextunity.brain.udb_reporter import BrainUdbReporter


@pytest.mark.asyncio
async def test_embedding_provider_failure_uses_local_app_and_deduplicates(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    reporter = BrainUdbReporter(application=UdbApplication(storage=store, enabled=True))

    await reporter.report_embedding_provider_failure(
        tenant_id="acme",
        job_id="job-1",
        lease_id="lease-1",
    )
    await reporter.report_embedding_provider_failure(
        tenant_id="acme",
        job_id="job-1",
        lease_id="lease-1",
    )

    with store.get_sqlite_connection() as db:
        count = db.execute("SELECT COUNT(*) FROM debug_case_occurrences").fetchone()
    assert count is not None and count[0] == 1


@pytest.mark.asyncio
async def test_local_reporter_gate_off_and_storage_failure_never_recurse(tmp_path: Path) -> None:
    disabled_store = SqliteBrainStore(db_path=str(tmp_path / "disabled.sqlite3"), vector_dim=8)
    disabled = BrainUdbReporter(application=UdbApplication(storage=disabled_store, enabled=False))
    await disabled.report_embedding_provider_failure(
        tenant_id="acme",
        job_id="job-1",
        lease_id="lease-1",
    )
    with disabled_store.get_sqlite_connection() as db:
        count = db.execute("SELECT COUNT(*) FROM debug_case_occurrences").fetchone()
    assert count is not None and count[0] == 0

    class _BrokenApplication(UdbApplication):
        def __init__(self) -> None:
            self.calls = 0

        async def report_fault_occurrence(self, _occurrence) -> None:
            self.calls += 1
            raise RuntimeError("storage unavailable")

    broken_application = _BrokenApplication()
    broken = BrainUdbReporter(application=broken_application)
    await broken.report_embedding_provider_failure(
        tenant_id="acme",
        job_id="job-1",
        lease_id="lease-1",
    )
    assert broken_application.calls == 1
