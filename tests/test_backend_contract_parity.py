"""Shared business-contract checks for SQLite and PostgreSQL stores."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from contextunity.core.udb import (
    DebugCaseQuery,
    FaultOccurrence,
    MitigationAttempt,
    RecoveryEvidence,
    ReopenDebugCase,
    ResolveDebugCase,
    UdbComparisonKey,
)

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.contracts import BrainStorageProtocol
from contextunity.brain.storage.postgres import PostgresBrainStore
from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)


@pytest_asyncio.fixture
async def postgres_store(brain_test_dsn: str) -> PostgresBrainStore:
    dsn = brain_test_dsn
    from psycopg import AsyncConnection, sql

    schema = f"backend_contract_{uuid4().hex}"
    store = PostgresBrainStore(dsn=dsn, schema=schema, pool_min_size=1, pool_max_size=1)
    try:
        await store.ensure_schema()
        yield store
    finally:
        try:
            await store.close()
        finally:
            admin = await AsyncConnection.connect(dsn, autocommit=True)
            try:
                _ = await admin.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
                )
            finally:
                await admin.close()


async def _assert_cell_contract(store: BrainStorageProtocol, *, tenant_id: str) -> None:
    upserted = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="A stable backend contract",
        metadata={"domain": "parity"},
        scope_path=f"{tenant_id}.memory",
        content_hash=f"sha256:{uuid4().hex}",
        source_type="auto_extract",
        source_ref="conversation-1",
        confidence=0.99,
        visibility="tenant",
    )
    assert set(upserted) == {
        "id",
        "tenant_id",
        "cell_kind",
        "source_type",
        "scope_path",
        "content_hash",
        "confidence",
        "visibility",
        "created_at",
        "updated_at",
    }
    assert upserted["confidence"] == 0.75

    rows = await store.query_cells(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        source_type="auto_extract",
        scope_path=f"{tenant_id}.memory",
        limit=10,
    )
    assert len(rows) == 1
    assert set(rows[0]) == {
        "id",
        "tenant_id",
        "cell_kind",
        "content",
        "metadata",
        "content_hash",
        "scope_path",
        "source_type",
        "source_ref",
        "confidence",
        "visibility",
    }
    assert rows[0]["id"] == upserted["id"]

    cell = await store.get_cell(
        tenant_id=tenant_id,
        cell_id=str(upserted["id"]),
        user_id="user-a",
    )
    assert cell is not None
    assert set(cell) == {
        "id",
        "tenant_id",
        "cell_kind",
        "content",
        "metadata",
        "content_hash",
        "source_type",
        "source_ref",
        "scope_path",
        "confidence",
        "visibility",
        "created_at",
        "updated_at",
    }
    assert (
        await store.get_cell(
            tenant_id=tenant_id,
            cell_id=str(upserted["id"]),
            user_id="user-b",
        )
        is None
    )

    source_owned = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="Source owned identity",
        source_type="auto_extract",
    )
    normalized_repeat = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content=" Source   owned\nidentity ",
        source_type="auto_extract",
    )
    other_producer = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="Source owned identity",
        source_type="synthesis",
    )
    assert normalized_repeat["id"] == source_owned["id"]
    assert normalized_repeat["content_hash"] == source_owned["content_hash"]
    assert other_producer["id"] != source_owned["id"]
    assert other_producer["content_hash"] != source_owned["content_hash"]

    documentation = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_id=f"documentation-{uuid4().hex}",
        cell_kind="documentation",
        content="Exact documentation cleanup contract",
        content_hash=f"sha256:{uuid4().hex}",
        source_type="documentation",
        metadata={"source_path": "docs/cleanup.md"},
    )
    conflict = await store.delete_documentation_cells(
        tenant_id=tenant_id,
        targets=[(str(documentation["id"]), "stale")],
    )
    assert conflict == {"status": "conflict", "deleted_count": 0, "expected_count": 1}
    deleted = await store.delete_documentation_cells(
        tenant_id=tenant_id,
        targets=[(str(documentation["id"]), str(documentation["content_hash"]))],
    )
    assert deleted == {"status": "deleted", "deleted_count": 1, "expected_count": 1}
    assert await store.get_cell(tenant_id=tenant_id, cell_id=str(documentation["id"])) is None


async def _assert_udb_mutation_idempotency(
    store: SqliteBrainStore | PostgresBrainStore,
    *,
    tenant_id: str,
) -> None:
    occurrence = FaultOccurrence(
        occurrence_id=uuid4(),
        tenant_id=tenant_id,
        producer_id="parity:udb",
        idempotency_key=f"occurrence:{uuid4()}",
        fingerprint_version="contextunity.udb-fingerprint/v1",
        fingerprint="a" * 64,
        fault_class="upstream_fault",
        operation_kind="brain_search",
        fault_code="brain.search.unavailable",
        comparison_key=UdbComparisonKey(
            tenant_id=tenant_id,
            operation_kind="brain_search",
            subject_ref="brain:search",
            capability_class="brain:search",
        ),
        trace_id=uuid4(),
        graph_run_id=uuid4(),
        occurred_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )
    case = await store.report_fault_occurrence(occurrence)
    assert await store.report_fault_occurrence(occurrence) == case
    attempt = MitigationAttempt(
        attempt_id=uuid4(),
        case_id=case.case_id,
        expected_case_revision=case.case_revision,
        kind="retry",
        idempotency_key=f"mitigation:{uuid4()}",
        attempted_at=datetime(2026, 7, 16, 12, 1, tzinfo=UTC),
    )
    mitigated = await store.report_mitigation_attempt(tenant_id=tenant_id, attempt=attempt)
    assert await store.report_mitigation_attempt(tenant_id=tenant_id, attempt=attempt) == mitigated
    with pytest.raises(BrainValidationError, match="conflicting mitigation idempotency key"):
        await store.report_mitigation_attempt(
            tenant_id=tenant_id,
            attempt=attempt.model_copy(
                update={"attempted_at": datetime(2026, 7, 16, 12, 1, 1, tzinfo=UTC)}
            ),
        )

    recovery = RecoveryEvidence(
        recovery_id=uuid4(),
        case_id=case.case_id,
        policy_version=case.policy_version,
        comparison_key=case.comparison_key,
        expected_case_revision=mitigated.case_revision,
        exposure_id=f"recovery:{uuid4()}",
        kind="comparable_success",
        verified_at=datetime(2026, 7, 16, 12, 2, tzinfo=UTC),
    )
    recovered = await store.report_recovery_evidence(tenant_id=tenant_id, evidence=recovery)
    assert await store.report_recovery_evidence(tenant_id=tenant_id, evidence=recovery) == recovered
    resolution = ResolveDebugCase(
        case_id=case.case_id,
        expected_case_revision=recovered.case_revision,
        resolution_id=f"resolution:{uuid4()}",
        resolved_at=datetime(2026, 7, 16, 12, 3, tzinfo=UTC),
    )
    resolved = await store.resolve_debug_case(tenant_id=tenant_id, command=resolution)
    assert await store.resolve_debug_case(tenant_id=tenant_id, command=resolution) == resolved
    with pytest.raises(BrainValidationError, match="conflicting resolution id"):
        await store.resolve_debug_case(
            tenant_id=tenant_id,
            command=resolution.model_copy(
                update={"resolved_at": datetime(2026, 7, 16, 12, 3, 1, tzinfo=UTC)}
            ),
        )

    reopen = ReopenDebugCase(
        case_id=case.case_id,
        expected_case_revision=resolved.case_revision,
        reopen_id=f"reopen:{uuid4()}",
        trigger_occurrence_id=occurrence.occurrence_id,
        reopened_at=datetime(2026, 7, 16, 12, 4, tzinfo=UTC),
    )
    reopened = await store.reopen_debug_case(tenant_id=tenant_id, command=reopen)
    assert await store.reopen_debug_case(tenant_id=tenant_id, command=reopen) == reopened
    with pytest.raises(BrainValidationError, match="conflicting reopen id"):
        await store.reopen_debug_case(
            tenant_id=tenant_id,
            command=reopen.model_copy(
                update={"reopened_at": datetime(2026, 7, 16, 12, 4, 1, tzinfo=UTC)}
            ),
        )

    detail = await store.get_debug_case_detail(
        tenant_id=tenant_id,
        case_id=case.case_id,
        history_limit=10,
    )
    assert detail is not None
    assert detail.case == reopened
    assert [item.trace_id for item in detail.occurrences] == [occurrence.trace_id]
    assert [item.attempt_id for item in detail.mitigations] == [attempt.attempt_id]
    assert [item.recovery_id for item in detail.recoveries] == [recovery.recovery_id]
    assert [item.transition_kind for item in detail.transitions] == ["resolved", "reopened"]
    assert await store.query_debug_cases(
        tenant_id=tenant_id,
        query=DebugCaseQuery(trace_id=occurrence.trace_id, limit=10),
    ) == [reopened]
    assert (
        await store.query_debug_cases(
            tenant_id=tenant_id,
            query=DebugCaseQuery(trace_id=uuid4(), limit=10),
        )
        == []
    )


async def _assert_embedding_transition_contract(
    store: BrainStorageProtocol,
    *,
    tenant_id: str,
) -> None:
    content_hash = f"sha256:{uuid4().hex}"
    cell = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_kind="document",
        content="Embedding transition contract",
        content_hash=content_hash,
        source_type="manual",
    )
    accepted = await store.enqueue_embedding_job(
        tenant_id=tenant_id,
        cell_id=str(cell["id"]),
        content_hash=content_hash,
        profile="default",
        max_pending=10,
    )
    assert accepted["status"] == "pending"
    assert accepted["accepted"] is True

    first = (await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60))[0]
    retry = await store.fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(first["job_id"]),
        lease_id=str(first["lease_id"]),
        error_code="provider_failure",
    )
    assert retry == {"status": "pending", "retryable": True, "attempt": 1}

    second = (await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60))[0]
    failed = await store.terminal_fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert failed == {"status": "failed", "attempt": 2}
    repeated = await store.terminal_fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert repeated == {"status": "failed", "attempt": 2, "idempotent": True}


@pytest.mark.asyncio
async def test_sqlite_business_contract(sqlite_store: SqliteBrainStore) -> None:
    tenant_id = f"sqlite-parity-{uuid4().hex}"
    await _assert_cell_contract(sqlite_store, tenant_id=tenant_id)
    await _assert_embedding_transition_contract(sqlite_store, tenant_id=tenant_id)
    await _assert_udb_mutation_idempotency(sqlite_store, tenant_id=tenant_id)


@pytest.mark.asyncio
async def test_sqlite_cell_operations_close_each_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-call SQLite operations must not accumulate file handles across a suite."""
    store = SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)
    opened: list[sqlite3.Connection] = []
    original_get_connection = store._get_connection

    def tracked_connection() -> sqlite3.Connection:
        connection = original_get_connection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(store, "_get_connection", tracked_connection)
    tenant_id = "sqlite-connection-lifecycle"
    cell = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_kind="fact",
        content="close the connection after a write",
        source_type="manual",
    )
    assert await store.query_cells(tenant_id=tenant_id) != []
    assert await store.get_cell(tenant_id=tenant_id, cell_id=str(cell["id"])) is not None
    assert await store.get_debug_case(tenant_id=tenant_id, case_id=uuid4()) is None
    documentation = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_id="documentation-connection-lifecycle",
        cell_kind="documentation",
        content="documentation connection lifecycle",
        content_hash="sha256:connection-lifecycle",
        source_type="documentation",
    )
    assert await store.delete_documentation_cells(
        tenant_id=tenant_id,
        targets=[(str(documentation["id"]), str(documentation["content_hash"]))],
    ) == {"status": "deleted", "deleted_count": 1, "expected_count": 1}

    assert len(opened) == 6
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


@pytest.mark.asyncio
async def test_postgres_business_contract(postgres_store: PostgresBrainStore) -> None:
    tenant_id = f"postgres-parity-{uuid4().hex}"
    await _assert_cell_contract(postgres_store, tenant_id=tenant_id)
    await _assert_embedding_transition_contract(postgres_store, tenant_id=tenant_id)
    await _assert_udb_mutation_idempotency(postgres_store, tenant_id=tenant_id)
