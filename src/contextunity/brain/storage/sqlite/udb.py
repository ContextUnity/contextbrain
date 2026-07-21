"""SQLite persistence for Brain-owned UniversalDebugBus evidence."""

from __future__ import annotations

import sqlite3
from hashlib import sha256
from json import dumps as canonical_dumps
from uuid import UUID

from contextunity.core.types import is_json_dict
from contextunity.core.udb import (
    DebugCase,
    DebugCaseDetail,
    DebugCaseMitigationView,
    DebugCaseOccurrenceView,
    DebugCaseQuery,
    DebugCaseRecoveryView,
    DebugCaseTransitionView,
    ErrorEvidencePolicyV1,
    FaultOccurrence,
    MitigationAttempt,
    RecoveryEvidence,
    ReopenDebugCase,
    ResolveDebugCase,
)

from contextunity.brain.core.exceptions import BrainValidationError

from .codecs import json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin


def _canonical_digest(
    model: FaultOccurrence
    | RecoveryEvidence
    | MitigationAttempt
    | ResolveDebugCase
    | ReopenDebugCase,
) -> str:
    """Hash immutable UDB input, ignoring only delivery-observation times."""
    payload = model.model_dump(mode="json")
    if isinstance(model, FaultOccurrence):
        payload.pop("occurred_at")
    elif isinstance(model, RecoveryEvidence):
        payload.pop("verified_at")
    return sha256(
        canonical_dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _debug_case_from_row(row: sqlite3.Row) -> DebugCase:
    """Decode one SQLite aggregate row through its closed L4 model."""
    comparison_raw = sqlite_cell(row, "comparison_key")
    comparison_key = json_loads(comparison_raw if isinstance(comparison_raw, str) else None)
    if not is_json_dict(comparison_key):
        raise BrainValidationError("stored DebugCase comparison key is malformed")
    return DebugCase.model_validate(
        {
            "case_id": sqlite_cell(row, "case_id"),
            "tenant_id": sqlite_cell(row, "tenant_id"),
            "fingerprint_version": sqlite_cell(row, "fingerprint_version"),
            "fingerprint": sqlite_cell(row, "fingerprint"),
            "fault_class": sqlite_cell(row, "fault_class"),
            "operation_kind": sqlite_cell(row, "operation_kind"),
            "policy_version": sqlite_cell(row, "policy_version"),
            "comparison_key": comparison_key,
            "state": sqlite_cell(row, "state"),
            "fault_count": sqlite_cell(row, "fault_count"),
            "success_count": sqlite_cell(row, "success_count"),
            "q_error": sqlite_cell(row, "q_error"),
            "case_revision": sqlite_cell(row, "case_revision"),
            "first_occurred_at": sqlite_cell(row, "first_occurred_at"),
            "last_occurred_at": sqlite_cell(row, "last_occurred_at"),
            "resolved_at": sqlite_cell(row, "resolved_at"),
        }
    )


def _case_columns() -> str:
    """Return the closed projection required to rebuild a ``DebugCase``."""
    return (
        "case_id, tenant_id, fingerprint_version, fingerprint, fault_class, "
        "operation_kind, policy_version, comparison_key, state, fault_count, "
        "success_count, q_error, case_revision, first_occurred_at, "
        "last_occurred_at, resolved_at"
    )


class UdbMixin(SqliteConnectionMixin):
    """SQLite UDB aggregate operations with database-enforced identities."""

    async def report_fault_occurrence(self, occurrence: FaultOccurrence) -> DebugCase:
        """Persist an immutable occurrence and atomically create/update its case."""
        occurrence_digest = _canonical_digest(occurrence)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing_occurrence = db.execute(
                    """
                    SELECT case_id, canonical_digest
                    FROM debug_case_occurrences
                    WHERE tenant_id = ? AND producer_id = ? AND idempotency_key = ?
                    """,
                    (occurrence.tenant_id, occurrence.producer_id, occurrence.idempotency_key),
                ).fetchone()
                if existing_occurrence is not None:
                    if sqlite_cell(existing_occurrence, "canonical_digest") != occurrence_digest:
                        raise BrainValidationError("conflicting fault occurrence idempotency key")
                    case_row = db.execute(
                        f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                        (
                            sqlite_cell(existing_occurrence, "case_id"),
                            occurrence.tenant_id,
                        ),
                    ).fetchone()
                    if case_row is None:
                        raise BrainValidationError("stored fault occurrence has no DebugCase")
                    db.rollback()
                    return _debug_case_from_row(case_row)

                case_row = db.execute(
                    f"""
                    SELECT {_case_columns()} FROM debug_cases
                    WHERE tenant_id = ? AND fingerprint_version = ? AND fingerprint = ?
                    """,
                    (
                        occurrence.tenant_id,
                        occurrence.fingerprint_version,
                        occurrence.fingerprint,
                    ),
                ).fetchone()
                if case_row is None:
                    case = DebugCase(
                        case_id=occurrence.occurrence_id,
                        tenant_id=occurrence.tenant_id,
                        fingerprint_version=occurrence.fingerprint_version,
                        fingerprint=occurrence.fingerprint,
                        fault_class=occurrence.fault_class,
                        operation_kind=occurrence.operation_kind,
                        policy_version=occurrence.policy_version,
                        comparison_key=occurrence.comparison_key,
                        state="open",
                        fault_count=1,
                        success_count=0,
                        q_error=ErrorEvidencePolicyV1().q_error(
                            fault_count=1,
                            success_count=0,
                        ),
                        case_revision=1,
                        first_occurred_at=occurrence.occurred_at,
                        last_occurred_at=occurrence.occurred_at,
                    )
                    db.execute(
                        """
                        INSERT INTO debug_cases (
                            case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                            operation_kind, policy_version, comparison_key, state, fault_count,
                            success_count, q_error, case_revision, first_occurred_at,
                            last_occurred_at, resolved_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(case.case_id),
                            case.tenant_id,
                            case.fingerprint_version,
                            case.fingerprint,
                            case.fault_class,
                            case.operation_kind,
                            case.policy_version,
                            json_dumps(case.comparison_key.model_dump(mode="json")),
                            case.state,
                            case.fault_count,
                            case.success_count,
                            case.q_error,
                            case.case_revision,
                            case.first_occurred_at.isoformat(),
                            case.last_occurred_at.isoformat(),
                            None,
                        ),
                    )
                else:
                    current = _debug_case_from_row(case_row)
                    if occurrence.occurred_at < current.last_occurred_at:
                        raise BrainValidationError("occurrence is out of order for DebugCase")
                    if (
                        current.fault_class != occurrence.fault_class
                        or current.operation_kind != occurrence.operation_kind
                        or current.policy_version != occurrence.policy_version
                        or current.comparison_key != occurrence.comparison_key
                    ):
                        raise BrainValidationError(
                            "fault occurrence conflicts with DebugCase identity"
                        )
                    fault_count = current.fault_count + 1
                    case = current.model_copy(
                        update={
                            "state": "open",
                            "fault_count": fault_count,
                            "q_error": ErrorEvidencePolicyV1().q_error(
                                fault_count=fault_count,
                                success_count=current.success_count,
                            ),
                            "case_revision": current.case_revision + 1,
                            "last_occurred_at": occurrence.occurred_at,
                            "resolved_at": None,
                        }
                    )
                    updated = db.execute(
                        """
                        UPDATE debug_cases
                        SET state = ?, fault_count = ?, q_error = ?, case_revision = ?,
                            last_occurred_at = ?, resolved_at = NULL
                        WHERE case_id = ? AND tenant_id = ? AND case_revision = ?
                        """,
                        (
                            case.state,
                            case.fault_count,
                            case.q_error,
                            case.case_revision,
                            case.last_occurred_at.isoformat(),
                            str(case.case_id),
                            case.tenant_id,
                            current.case_revision,
                        ),
                    )
                    if updated.rowcount != 1:
                        raise BrainValidationError("stale DebugCase revision")

                db.execute(
                    """
                    INSERT INTO debug_case_occurrences (
                        occurrence_id, case_id, tenant_id, producer_id, idempotency_key,
                        fingerprint_version, fingerprint, fault_class, operation_kind, fault_code,
                        policy_version, comparison_key, trace_id, graph_run_id, node_id, step_id,
                        occurred_at, canonical_digest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(occurrence.occurrence_id),
                        str(case.case_id),
                        occurrence.tenant_id,
                        occurrence.producer_id,
                        occurrence.idempotency_key,
                        occurrence.fingerprint_version,
                        occurrence.fingerprint,
                        occurrence.fault_class,
                        occurrence.operation_kind,
                        occurrence.fault_code,
                        occurrence.policy_version,
                        json_dumps(occurrence.comparison_key.model_dump(mode="json")),
                        str(occurrence.trace_id) if occurrence.trace_id is not None else None,
                        str(occurrence.graph_run_id)
                        if occurrence.graph_run_id is not None
                        else None,
                        occurrence.node_id,
                        str(occurrence.step_id) if occurrence.step_id is not None else None,
                        occurrence.occurred_at.isoformat(),
                        occurrence_digest,
                    ),
                )
                db.commit()
                return case
            except Exception:
                db.rollback()
                raise

    async def get_debug_case(self, *, tenant_id: str, case_id: UUID) -> DebugCase | None:
        """Read one tenant-owned DebugCase without widening the storage scope."""
        with self._get_connection() as db:
            row = db.execute(
                f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                (str(case_id), tenant_id),
            ).fetchone()
        return _debug_case_from_row(row) if row is not None else None

    async def get_debug_case_detail(
        self,
        *,
        tenant_id: str,
        case_id: UUID,
        history_limit: int,
    ) -> DebugCaseDetail | None:
        """Read one tenant-owned case and independently bounded closed history."""
        with self._get_connection() as db:
            # One explicit read transaction pins the aggregate and all child
            # collections to the same SQLite snapshot.
            db.execute("BEGIN")
            case_row = db.execute(
                f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                (str(case_id), tenant_id),
            ).fetchone()
            if case_row is None:
                return None
            child_params = (str(case_id), tenant_id, history_limit)
            occurrence_rows = db.execute(
                """
                SELECT occurrence_id, fault_code, trace_id, graph_run_id, node_id,
                       step_id, occurred_at
                FROM debug_case_occurrences
                WHERE case_id = ? AND tenant_id = ?
                ORDER BY occurred_at ASC, occurrence_id ASC
                LIMIT ?
                """,
                child_params,
            ).fetchall()
            mitigation_rows = db.execute(
                """
                SELECT attempt_id, expected_case_revision, kind, attempted_at
                FROM debug_case_mitigations
                WHERE case_id = ? AND tenant_id = ?
                ORDER BY attempted_at ASC, attempt_id ASC
                LIMIT ?
                """,
                child_params,
            ).fetchall()
            recovery_rows = db.execute(
                """
                SELECT recovery_id, expected_case_revision, exposure_id, kind, verified_at
                FROM debug_case_recoveries
                WHERE case_id = ? AND tenant_id = ?
                ORDER BY verified_at ASC, recovery_id ASC
                LIMIT ?
                """,
                child_params,
            ).fetchall()
            transition_rows = db.execute(
                """
                SELECT transition_id, transition_kind, expected_case_revision,
                       trigger_occurrence_id, transitioned_at
                FROM debug_case_transitions
                WHERE case_id = ? AND tenant_id = ?
                ORDER BY transitioned_at ASC, transition_id ASC
                LIMIT ?
                """,
                child_params,
            ).fetchall()

        return DebugCaseDetail(
            case=_debug_case_from_row(case_row),
            occurrences=tuple(
                DebugCaseOccurrenceView.model_validate(
                    {key: sqlite_cell(row, key) for key in row.keys()}
                )
                for row in occurrence_rows
            ),
            mitigations=tuple(
                DebugCaseMitigationView.model_validate(
                    {key: sqlite_cell(row, key) for key in row.keys()}
                )
                for row in mitigation_rows
            ),
            recoveries=tuple(
                DebugCaseRecoveryView.model_validate(
                    {key: sqlite_cell(row, key) for key in row.keys()}
                )
                for row in recovery_rows
            ),
            transitions=tuple(
                DebugCaseTransitionView.model_validate(
                    {key: sqlite_cell(row, key) for key in row.keys()}
                )
                for row in transition_rows
            ),
        )

    async def report_recovery_evidence(
        self,
        *,
        tenant_id: str,
        evidence: RecoveryEvidence,
    ) -> DebugCase:
        """Record one verified comparable success with revision-CAS protection."""
        evidence_digest = _canonical_digest(evidence)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute(
                    """
                    SELECT canonical_digest FROM debug_case_recoveries
                    WHERE case_id = ? AND exposure_id = ?
                    """,
                    (str(evidence.case_id), evidence.exposure_id),
                ).fetchone()
                if existing is not None:
                    if sqlite_cell(existing, "canonical_digest") != evidence_digest:
                        raise BrainValidationError("conflicting recovery exposure id")
                    case_row = db.execute(
                        f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                        (str(evidence.case_id), tenant_id),
                    ).fetchone()
                    if case_row is None:
                        raise BrainValidationError("stored recovery has no tenant-owned DebugCase")
                    db.rollback()
                    return _debug_case_from_row(case_row)

                case_row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(evidence.case_id), tenant_id),
                ).fetchone()
                if case_row is None:
                    raise BrainValidationError("DebugCase not found for tenant")
                current = _debug_case_from_row(case_row)
                if current.state != "open":
                    raise BrainValidationError("resolved DebugCase cannot accept recovery evidence")
                if current.case_revision != evidence.expected_case_revision:
                    raise BrainValidationError("stale DebugCase revision")
                if (
                    current.policy_version != evidence.policy_version
                    or current.comparison_key != evidence.comparison_key
                ):
                    raise BrainValidationError("recovery evidence is not comparable with DebugCase")

                success_count = current.success_count + 1
                case = current.model_copy(
                    update={
                        "success_count": success_count,
                        "q_error": ErrorEvidencePolicyV1().q_error(
                            fault_count=current.fault_count,
                            success_count=success_count,
                        ),
                        "case_revision": current.case_revision + 1,
                    }
                )
                updated = db.execute(
                    """
                    UPDATE debug_cases
                    SET success_count = ?, q_error = ?, case_revision = ?
                    WHERE case_id = ? AND tenant_id = ? AND case_revision = ?
                    """,
                    (
                        case.success_count,
                        case.q_error,
                        case.case_revision,
                        str(case.case_id),
                        case.tenant_id,
                        current.case_revision,
                    ),
                )
                if updated.rowcount != 1:
                    raise BrainValidationError("stale DebugCase revision")
                db.execute(
                    """
                    INSERT INTO debug_case_recoveries (
                        recovery_id, case_id, tenant_id, policy_version, comparison_key,
                        expected_case_revision, exposure_id, kind, verified_at, canonical_digest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(evidence.recovery_id),
                        str(evidence.case_id),
                        tenant_id,
                        evidence.policy_version,
                        json_dumps(evidence.comparison_key.model_dump(mode="json")),
                        evidence.expected_case_revision,
                        evidence.exposure_id,
                        evidence.kind,
                        evidence.verified_at.isoformat(),
                        evidence_digest,
                    ),
                )
                db.commit()
                return case
            except Exception:
                db.rollback()
                raise

    async def report_mitigation_attempt(
        self,
        *,
        tenant_id: str,
        attempt: MitigationAttempt,
    ) -> DebugCase:
        """Append an idempotent mitigation attempt and advance case revision."""
        attempt_digest = _canonical_digest(attempt)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute(
                    "SELECT attempt_id, canonical_digest FROM debug_case_mitigations "
                    "WHERE case_id = ? AND idempotency_key = ?",
                    (str(attempt.case_id), attempt.idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        sqlite_cell(existing, "attempt_id") != str(attempt.attempt_id)
                        or sqlite_cell(existing, "canonical_digest") != attempt_digest
                    ):
                        raise BrainValidationError("conflicting mitigation idempotency key")
                    row = db.execute(
                        f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                        (str(attempt.case_id), tenant_id),
                    ).fetchone()
                    if row is None:
                        raise BrainValidationError(
                            "stored mitigation has no tenant-owned DebugCase"
                        )
                    db.rollback()
                    return _debug_case_from_row(row)
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(attempt.case_id), tenant_id),
                ).fetchone()
                if row is None:
                    raise BrainValidationError("DebugCase not found for tenant")
                current = _debug_case_from_row(row)
                if (
                    current.state != "open"
                    or current.case_revision != attempt.expected_case_revision
                ):
                    raise BrainValidationError("stale or resolved DebugCase revision")
                db.execute(
                    """
                    INSERT INTO debug_case_mitigations (
                        attempt_id, case_id, tenant_id, expected_case_revision,
                        kind, idempotency_key, attempted_at, canonical_digest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(attempt.attempt_id),
                        str(attempt.case_id),
                        tenant_id,
                        attempt.expected_case_revision,
                        attempt.kind,
                        attempt.idempotency_key,
                        attempt.attempted_at.isoformat(),
                        attempt_digest,
                    ),
                )
                updated = db.execute(
                    "UPDATE debug_cases SET case_revision = case_revision + 1 WHERE case_id = ? AND tenant_id = ? AND case_revision = ?",
                    (str(attempt.case_id), tenant_id, attempt.expected_case_revision),
                )
                if updated.rowcount != 1:
                    raise BrainValidationError("stale DebugCase revision")
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(attempt.case_id), tenant_id),
                ).fetchone()
                db.commit()
                if row is None:
                    raise BrainValidationError("DebugCase disappeared after mitigation")
                return _debug_case_from_row(row)
            except Exception:
                db.rollback()
                raise

    async def resolve_debug_case(
        self,
        *,
        tenant_id: str,
        command: ResolveDebugCase,
    ) -> DebugCase:
        """Resolve an open case only after comparable recovery evidence exists."""
        command_digest = _canonical_digest(command)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute(
                    "SELECT transition_kind, case_id, canonical_digest "
                    "FROM debug_case_transitions WHERE transition_id = ?",
                    (command.resolution_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        sqlite_cell(existing, "transition_kind") != "resolved"
                        or sqlite_cell(existing, "case_id") != str(command.case_id)
                        or sqlite_cell(existing, "canonical_digest") != command_digest
                    ):
                        raise BrainValidationError("conflicting resolution id")
                    row = db.execute(
                        f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                        (str(command.case_id), tenant_id),
                    ).fetchone()
                    if row is None:
                        raise BrainValidationError(
                            "stored resolution has no tenant-owned DebugCase"
                        )
                    db.rollback()
                    return _debug_case_from_row(row)
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(command.case_id), tenant_id),
                ).fetchone()
                if row is None:
                    raise BrainValidationError("DebugCase not found for tenant")
                current = _debug_case_from_row(row)
                if (
                    current.state != "open"
                    or current.case_revision != command.expected_case_revision
                ):
                    raise BrainValidationError("stale or resolved DebugCase revision")
                if (
                    current.success_count
                    < ErrorEvidencePolicyV1(version=current.policy_version).minimum_success_count
                ):
                    raise BrainValidationError(
                        "DebugCase requires comparable recovery before resolution"
                    )
                db.execute(
                    """
                    INSERT INTO debug_case_transitions (
                        transition_id, case_id, tenant_id, transition_kind,
                        expected_case_revision, trigger_occurrence_id, transitioned_at,
                        canonical_digest
                    ) VALUES (?, ?, ?, 'resolved', ?, NULL, ?, ?)
                    """,
                    (
                        command.resolution_id,
                        str(command.case_id),
                        tenant_id,
                        command.expected_case_revision,
                        command.resolved_at.isoformat(),
                        command_digest,
                    ),
                )
                updated = db.execute(
                    "UPDATE debug_cases SET state = 'resolved', resolved_at = ?, case_revision = case_revision + 1 WHERE case_id = ? AND tenant_id = ? AND case_revision = ?",
                    (
                        command.resolved_at.isoformat(),
                        str(command.case_id),
                        tenant_id,
                        command.expected_case_revision,
                    ),
                )
                if updated.rowcount != 1:
                    raise BrainValidationError("stale DebugCase revision")
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(command.case_id), tenant_id),
                ).fetchone()
                db.commit()
                if row is None:
                    raise BrainValidationError("DebugCase disappeared after resolution")
                return _debug_case_from_row(row)
            except Exception:
                db.rollback()
                raise

    async def reopen_debug_case(
        self,
        *,
        tenant_id: str,
        command: ReopenDebugCase,
    ) -> DebugCase:
        """Explicitly reopen a resolved case linked to a persisted occurrence."""
        command_digest = _canonical_digest(command)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute(
                    "SELECT transition_kind, case_id, canonical_digest "
                    "FROM debug_case_transitions WHERE transition_id = ?",
                    (command.reopen_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        sqlite_cell(existing, "transition_kind") != "reopened"
                        or sqlite_cell(existing, "case_id") != str(command.case_id)
                        or sqlite_cell(existing, "canonical_digest") != command_digest
                    ):
                        raise BrainValidationError("conflicting reopen id")
                    row = db.execute(
                        f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                        (str(command.case_id), tenant_id),
                    ).fetchone()
                    if row is None:
                        raise BrainValidationError("stored reopen has no tenant-owned DebugCase")
                    db.rollback()
                    return _debug_case_from_row(row)
                trigger = db.execute(
                    "SELECT occurrence_id FROM debug_case_occurrences WHERE occurrence_id = ? AND case_id = ? AND tenant_id = ?",
                    (str(command.trigger_occurrence_id), str(command.case_id), tenant_id),
                ).fetchone()
                if trigger is None:
                    raise BrainValidationError(
                        "reopen trigger occurrence not found for tenant case"
                    )
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(command.case_id), tenant_id),
                ).fetchone()
                if row is None:
                    raise BrainValidationError("DebugCase not found for tenant")
                current = _debug_case_from_row(row)
                if (
                    current.state != "resolved"
                    or current.case_revision != command.expected_case_revision
                ):
                    raise BrainValidationError("stale or open DebugCase revision")
                db.execute(
                    """
                    INSERT INTO debug_case_transitions (
                        transition_id, case_id, tenant_id, transition_kind,
                        expected_case_revision, trigger_occurrence_id, transitioned_at,
                        canonical_digest
                    ) VALUES (?, ?, ?, 'reopened', ?, ?, ?, ?)
                    """,
                    (
                        command.reopen_id,
                        str(command.case_id),
                        tenant_id,
                        command.expected_case_revision,
                        str(command.trigger_occurrence_id),
                        command.reopened_at.isoformat(),
                        command_digest,
                    ),
                )
                updated = db.execute(
                    "UPDATE debug_cases SET state = 'open', resolved_at = NULL, case_revision = case_revision + 1 WHERE case_id = ? AND tenant_id = ? AND case_revision = ?",
                    (str(command.case_id), tenant_id, command.expected_case_revision),
                )
                if updated.rowcount != 1:
                    raise BrainValidationError("stale DebugCase revision")
                row = db.execute(
                    f"SELECT {_case_columns()} FROM debug_cases WHERE case_id = ? AND tenant_id = ?",
                    (str(command.case_id), tenant_id),
                ).fetchone()
                db.commit()
                if row is None:
                    raise BrainValidationError("DebugCase disappeared after reopen")
                return _debug_case_from_row(row)
            except Exception:
                db.rollback()
                raise

    async def query_debug_cases(
        self,
        *,
        tenant_id: str,
        query: DebugCaseQuery,
        recurring_only: bool = False,
    ) -> list[DebugCase]:
        """Return a bounded tenant-owned projection ordered by recent evidence."""
        clauses = ["tenant_id = ?", "fault_count >= ?"]
        params: list[object] = [
            tenant_id,
            max(query.minimum_fault_count, 2 if recurring_only else 1),
        ]
        if query.state is not None:
            clauses.append("state = ?")
            params.append(query.state)
        occurrence_filters: list[str] = []
        if query.trace_id is not None:
            occurrence_filters.append("occurrence.trace_id = ?")
            params.append(str(query.trace_id))
        if query.graph_run_id is not None:
            occurrence_filters.append("occurrence.graph_run_id = ?")
            params.append(str(query.graph_run_id))
        if occurrence_filters:
            clauses.append(
                "EXISTS (SELECT 1 FROM debug_case_occurrences AS occurrence "
                "WHERE occurrence.case_id = debug_cases.case_id "
                "AND occurrence.tenant_id = debug_cases.tenant_id AND "
                + " AND ".join(occurrence_filters)
                + ")"
            )
        params.append(query.limit)
        with self._get_connection() as db:
            rows = db.execute(
                f"SELECT {_case_columns()} FROM debug_cases WHERE {' AND '.join(clauses)} ORDER BY fault_count DESC, last_occurred_at DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [_debug_case_from_row(row) for row in rows]


__all__ = ["UdbMixin"]
