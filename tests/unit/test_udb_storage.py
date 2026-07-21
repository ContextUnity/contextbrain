"""SQLite contract tests for the Brain-owned UDB persistence seam."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
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
from contextunity.brain.storage.sqlite import SqliteBrainStore


def _occurrence(*, idempotency_key: str = "router-run-1:1") -> FaultOccurrence:
    return FaultOccurrence(
        occurrence_id=uuid4(),
        tenant_id="acme",
        producer_id="router:memory-read",
        idempotency_key=idempotency_key,
        fingerprint_version="contextunity.udb-fingerprint/v1",
        fingerprint="a" * 64,
        fault_class="upstream_fault",
        operation_kind="brain_search",
        fault_code="brain.search.unavailable",
        comparison_key=UdbComparisonKey(
            tenant_id="acme",
            operation_kind="brain_search",
            subject_ref="cell:ab12",
            capability_class="brain:search",
        ),
        occurred_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )


@pytest.fixture
def store(tmp_path: Path) -> SqliteBrainStore:
    """Create an isolated Brain SQLite store with its current schema."""
    return SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)


def test_occurrence_deduplicates_then_recurs_without_cross_tenant_read(
    store: SqliteBrainStore,
) -> None:
    """One idempotent occurrence stays one fact; a new one increments its case."""
    occurrence = _occurrence()
    first = asyncio.run(store.report_fault_occurrence(occurrence))
    duplicate = asyncio.run(store.report_fault_occurrence(occurrence))
    retried_delivery = asyncio.run(
        store.report_fault_occurrence(
            occurrence.model_copy(update={"occurred_at": datetime(2026, 7, 16, 12, 1, tzinfo=UTC)})
        )
    )
    recurrent = asyncio.run(
        store.report_fault_occurrence(
            occurrence.model_copy(
                update={"occurrence_id": uuid4(), "idempotency_key": "router-run-1:2"}
            )
        )
    )

    assert first.fault_count == 1
    assert duplicate.fault_count == 1
    assert retried_delivery.fault_count == 1
    with pytest.raises(BrainValidationError, match="conflicting fault occurrence idempotency key"):
        asyncio.run(
            store.report_fault_occurrence(occurrence.model_copy(update={"fingerprint": "b" * 64}))
        )
    assert recurrent.fault_count == 2
    assert recurrent.case_revision == 2
    assert recurrent.q_error == pytest.approx(0.75)
    assert asyncio.run(store.get_debug_case(tenant_id="other", case_id=first.case_id)) is None


def test_detail_opens_read_snapshot_before_assembling_children(
    store: SqliteBrainStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = asyncio.run(store.report_fault_occurrence(_occurrence()))
    original = store._get_connection
    statements: list[str] = []

    class _TrackedConnection:
        def __init__(self, connection):
            self._connection = connection

        def execute(self, statement, parameters=()):
            statements.append(statement.strip())
            return self._connection.execute(statement, parameters)

        def __getattr__(self, name):
            return getattr(self._connection, name)

    @contextmanager
    def _tracked_connection():
        with original() as connection:
            yield _TrackedConnection(connection)

    monkeypatch.setattr(store, "_get_connection", _tracked_connection)
    detail = asyncio.run(
        store.get_debug_case_detail(tenant_id="acme", case_id=case.case_id, history_limit=10)
    )

    assert detail is not None
    assert statements[0] == "BEGIN"


def test_trace_and_graph_run_filters_must_match_the_same_occurrence(
    store: SqliteBrainStore,
) -> None:
    trace_a = uuid4()
    trace_b = uuid4()
    run_a = uuid4()
    run_b = uuid4()
    first_occurrence = _occurrence().model_copy(update={"trace_id": trace_a, "graph_run_id": run_a})
    case = asyncio.run(store.report_fault_occurrence(first_occurrence))
    second_occurrence = first_occurrence.model_copy(
        update={
            "occurrence_id": uuid4(),
            "idempotency_key": "router-run-1:2",
            "trace_id": trace_b,
            "graph_run_id": run_b,
        }
    )
    asyncio.run(store.report_fault_occurrence(second_occurrence))

    split_match = asyncio.run(
        store.query_debug_cases(
            tenant_id="acme",
            query=DebugCaseQuery(trace_id=trace_a, graph_run_id=run_b),
        )
    )
    paired_match = asyncio.run(
        store.query_debug_cases(
            tenant_id="acme",
            query=DebugCaseQuery(trace_id=trace_a, graph_run_id=run_a),
        )
    )

    assert split_match == []
    assert [item.case_id for item in paired_match] == [case.case_id]


def test_recovery_requires_current_case_revision_and_matching_key(store: SqliteBrainStore) -> None:
    """Only matching comparable evidence can update a current DebugCase."""
    case = asyncio.run(store.report_fault_occurrence(_occurrence()))
    recovery = RecoveryEvidence(
        recovery_id=uuid4(),
        case_id=case.case_id,
        policy_version="contextunity.error-evidence/v1",
        comparison_key=case.comparison_key,
        expected_case_revision=case.case_revision,
        exposure_id="router-run-1:retry-1",
        verified_at=datetime(2026, 7, 16, 12, 1, tzinfo=UTC),
    )

    unrelated = recovery.model_copy(
        update={
            "recovery_id": uuid4(),
            "exposure_id": "router-run-1:unrelated",
            "comparison_key": recovery.comparison_key.model_copy(
                update={"capability_class": "brain:other"}
            ),
        }
    )
    with pytest.raises(BrainValidationError, match="not comparable"):
        asyncio.run(store.report_recovery_evidence(tenant_id="acme", evidence=unrelated))

    recovered = asyncio.run(store.report_recovery_evidence(tenant_id="acme", evidence=recovery))
    assert recovered.success_count == 1
    assert recovered.case_revision == 2
    assert recovered.q_error == pytest.approx(0.5)

    retried_delivery = recovery.model_copy(
        update={"verified_at": datetime(2026, 7, 16, 12, 2, tzinfo=UTC)}
    )
    assert (
        asyncio.run(store.report_recovery_evidence(tenant_id="acme", evidence=retried_delivery))
        == recovered
    )

    stale = recovery.model_copy(
        update={"recovery_id": uuid4(), "exposure_id": "router-run-1:retry-2"}
    )
    with pytest.raises(BrainValidationError, match="stale DebugCase revision"):
        asyncio.run(store.report_recovery_evidence(tenant_id="acme", evidence=stale))


def test_lifecycle_mutations_are_revision_bound_and_queries_are_bounded(
    store: SqliteBrainStore,
) -> None:
    occurrence = _occurrence()
    case = asyncio.run(store.report_fault_occurrence(occurrence))
    attempt = MitigationAttempt(
        attempt_id=uuid4(),
        case_id=case.case_id,
        expected_case_revision=case.case_revision,
        kind="retry",
        idempotency_key="mitigation-1",
        attempted_at=datetime(2026, 7, 16, 12, 1, tzinfo=UTC),
    )
    mitigated = asyncio.run(store.report_mitigation_attempt(tenant_id="acme", attempt=attempt))
    assert (
        asyncio.run(store.report_mitigation_attempt(tenant_id="acme", attempt=attempt)) == mitigated
    )
    with pytest.raises(BrainValidationError, match="conflicting mitigation idempotency key"):
        asyncio.run(
            store.report_mitigation_attempt(
                tenant_id="acme",
                attempt=attempt.model_copy(
                    update={"attempted_at": datetime(2026, 7, 16, 12, 1, 1, tzinfo=UTC)}
                ),
            )
        )
    recovery = RecoveryEvidence(
        recovery_id=uuid4(),
        case_id=case.case_id,
        policy_version=case.policy_version,
        comparison_key=case.comparison_key,
        expected_case_revision=mitigated.case_revision,
        exposure_id="retry-success",
        verified_at=datetime(2026, 7, 16, 12, 2, tzinfo=UTC),
    )
    recovered = asyncio.run(store.report_recovery_evidence(tenant_id="acme", evidence=recovery))
    resolution = ResolveDebugCase(
        case_id=case.case_id,
        expected_case_revision=recovered.case_revision,
        resolution_id="resolution-1",
        resolved_at=datetime(2026, 7, 16, 12, 3, tzinfo=UTC),
    )
    resolved = asyncio.run(store.resolve_debug_case(tenant_id="acme", command=resolution))
    assert resolved.state == "resolved"
    assert asyncio.run(store.resolve_debug_case(tenant_id="acme", command=resolution)) == resolved
    with pytest.raises(BrainValidationError, match="conflicting resolution id"):
        asyncio.run(
            store.resolve_debug_case(
                tenant_id="acme",
                command=resolution.model_copy(
                    update={"resolved_at": datetime(2026, 7, 16, 12, 3, 1, tzinfo=UTC)}
                ),
            )
        )
    reopen = ReopenDebugCase(
        case_id=case.case_id,
        expected_case_revision=resolved.case_revision,
        reopen_id="reopen-1",
        trigger_occurrence_id=occurrence.occurrence_id,
        reopened_at=datetime(2026, 7, 16, 12, 4, tzinfo=UTC),
    )
    reopened = asyncio.run(store.reopen_debug_case(tenant_id="acme", command=reopen))
    assert reopened.state == "open"
    assert asyncio.run(store.reopen_debug_case(tenant_id="acme", command=reopen)) == reopened
    with pytest.raises(BrainValidationError, match="conflicting reopen id"):
        asyncio.run(
            store.reopen_debug_case(
                tenant_id="acme",
                command=reopen.model_copy(
                    update={"reopened_at": datetime(2026, 7, 16, 12, 4, 1, tzinfo=UTC)}
                ),
            )
        )
    cases = asyncio.run(store.query_debug_cases(tenant_id="acme", query=DebugCaseQuery(limit=1)))
    assert [item.case_id for item in cases] == [case.case_id]
    assert (
        asyncio.run(
            store.query_debug_cases(
                tenant_id="other", query=DebugCaseQuery(limit=1), recurring_only=True
            )
        )
        == []
    )

    detail = asyncio.run(
        store.get_debug_case_detail(tenant_id="acme", case_id=case.case_id, history_limit=10)
    )
    assert detail is not None
    assert detail.case == reopened
    assert [item.occurrence_id for item in detail.occurrences] == [occurrence.occurrence_id]
    assert [item.attempt_id for item in detail.mitigations] == [attempt.attempt_id]
    assert [item.recovery_id for item in detail.recoveries] == [recovery.recovery_id]
    assert [item.transition_kind for item in detail.transitions] == ["resolved", "reopened"]
    assert (
        asyncio.run(
            store.get_debug_case_detail(tenant_id="other", case_id=case.case_id, history_limit=10)
        )
        is None
    )


def test_occurrence_rejects_out_of_order_time_and_sqlite_rejects_bad_identity(
    store: SqliteBrainStore,
) -> None:
    """Temporal and fingerprint invariants are enforced before durable mutation."""
    occurrence = _occurrence()
    case = asyncio.run(store.report_fault_occurrence(occurrence))
    later = occurrence.model_copy(
        update={
            "occurrence_id": uuid4(),
            "idempotency_key": "router-run-1:later",
            "occurred_at": datetime(2026, 7, 16, 12, 2, tzinfo=UTC),
        }
    )
    asyncio.run(store.report_fault_occurrence(later))
    older = occurrence.model_copy(
        update={
            "occurrence_id": uuid4(),
            "idempotency_key": "router-run-1:older",
            "occurred_at": datetime(2026, 7, 16, 12, 1, tzinfo=UTC),
        }
    )
    with pytest.raises(BrainValidationError, match="out of order for DebugCase"):
        asyncio.run(store.report_fault_occurrence(older))

    before_first = occurrence.model_copy(
        update={
            "occurrence_id": uuid4(),
            "idempotency_key": "router-run-1:before-first",
            "occurred_at": datetime(2026, 7, 16, 11, 59, tzinfo=UTC),
        }
    )
    with pytest.raises(BrainValidationError, match="out of order for DebugCase"):
        asyncio.run(store.report_fault_occurrence(before_first))

    with store.get_sqlite_connection() as db:
        with pytest.raises(Exception):
            db.execute(
                """
                INSERT INTO debug_cases (
                    case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                    operation_kind, policy_version, comparison_key, state, fault_count,
                    success_count, q_error, case_revision, first_occurred_at, last_occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    "acme",
                    "invalid-version",
                    "G" * 64,
                    "upstream_fault",
                    "brain_search",
                    "contextunity.error-evidence/v1",
                    "{}",
                    "open",
                    1,
                    0,
                    2 / 3,
                    1,
                    "2026-07-16T12:00:00+00:00",
                    "2026-07-16T12:00:00+00:00",
                ),
            )
        case_row = db.execute(
            "SELECT tenant_id, case_id FROM debug_cases WHERE case_id = ?", (str(case.case_id),)
        ).fetchone()
        assert case_row is not None
        with pytest.raises(Exception):
            db.execute(
                """
                INSERT INTO debug_case_occurrences (
                    occurrence_id, case_id, tenant_id, producer_id, idempotency_key,
                    fingerprint_version, fingerprint, fault_class, operation_kind, fault_code,
                    policy_version, comparison_key, occurred_at, canonical_digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    str(case.case_id),
                    "other-tenant",
                    "router:test",
                    "cross-tenant-child",
                    "contextunity.udb-fingerprint/v1",
                    "a" * 64,
                    "upstream_fault",
                    "brain_search",
                    "brain.search.unavailable",
                    "contextunity.error-evidence/v1",
                    "{}",
                    "2026-07-16T12:00:01+00:00",
                    "b" * 64,
                ),
            )
        with pytest.raises(Exception):
            db.execute(
                """
                INSERT INTO debug_case_transitions (
                    transition_id, case_id, tenant_id, transition_kind,
                    expected_case_revision, trigger_occurrence_id, transitioned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "forged-reopen",
                    str(case.case_id),
                    "acme",
                    "reopened",
                    case.case_revision,
                    str(uuid4()),
                    "2026-07-16T12:00:02+00:00",
                ),
            )
