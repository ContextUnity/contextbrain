"""PostgreSQL persistence for Brain-owned UniversalDebugBus evidence."""

from __future__ import annotations

from hashlib import sha256
from json import dumps as canonical_dumps
from typing import TypeAlias
from uuid import UUID

from contextunity.core.types import JsonDict
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
from psycopg.types.json import Jsonb

from contextunity.brain.core.exceptions import BrainValidationError

from .base import PostgresStoreBase
from .helpers import PgConnection, fetch_all

_UdbItem: TypeAlias = (
    FaultOccurrence | RecoveryEvidence | MitigationAttempt | ResolveDebugCase | ReopenDebugCase
)


def _canonical_digest(model: _UdbItem) -> str:
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


def _debug_case_from_row(row: JsonDict) -> DebugCase:
    """Validate a storage projection through the shared closed aggregate model."""
    return DebugCase.model_validate(row)


async def _lock(conn: PgConnection, value: str) -> None:
    """Serialize one idempotency/case identity without adding a lock table."""
    _ = await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%(value)s))",
        {"value": value},
    )


class UdbMixin(PostgresStoreBase):
    """Postgres UDB aggregate operations with RLS and transactional CAS."""

    async def report_fault_occurrence(self, occurrence: FaultOccurrence) -> DebugCase:
        """Persist one occurrence and atomically correlate it into a DebugCase."""
        occurrence_digest = _canonical_digest(occurrence)
        idempotency_lock = (
            f"udb-occurrence:{occurrence.tenant_id}:{occurrence.producer_id}:"
            f"{occurrence.idempotency_key}"
        )
        case_lock = (
            f"udb-case:{occurrence.tenant_id}:{occurrence.fingerprint_version}:"
            f"{occurrence.fingerprint}"
        )
        async with await self.tenant_connection(occurrence.tenant_id) as conn:
            await _lock(conn, idempotency_lock)
            existing_occurrence = await fetch_all(
                conn,
                """
                SELECT case_id, canonical_digest
                FROM debug_case_occurrences
                WHERE tenant_id = %(tenant_id)s
                  AND producer_id = %(producer_id)s
                  AND idempotency_key = %(idempotency_key)s
                """,
                {
                    "tenant_id": occurrence.tenant_id,
                    "producer_id": occurrence.producer_id,
                    "idempotency_key": occurrence.idempotency_key,
                },
            )
            if existing_occurrence:
                if existing_occurrence[0].get("canonical_digest") != occurrence_digest:
                    raise BrainValidationError("conflicting fault occurrence idempotency key")
                rows = await fetch_all(
                    conn,
                    """
                    SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                           operation_kind, policy_version, comparison_key, state, fault_count,
                           success_count, q_error, case_revision, first_occurred_at,
                           last_occurred_at, resolved_at
                    FROM debug_cases
                    WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                    """,
                    {
                        "case_id": existing_occurrence[0].get("case_id"),
                        "tenant_id": occurrence.tenant_id,
                    },
                )
                if not rows:
                    raise BrainValidationError("stored fault occurrence has no DebugCase")
                return _debug_case_from_row(rows[0])

            await _lock(conn, case_lock)
            case_rows = await fetch_all(
                conn,
                """
                SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                       operation_kind, policy_version, comparison_key, state, fault_count,
                       success_count, q_error, case_revision, first_occurred_at,
                       last_occurred_at, resolved_at
                FROM debug_cases
                WHERE tenant_id = %(tenant_id)s
                  AND fingerprint_version = %(fingerprint_version)s
                  AND fingerprint = %(fingerprint)s
                FOR UPDATE
                """,
                {
                    "tenant_id": occurrence.tenant_id,
                    "fingerprint_version": occurrence.fingerprint_version,
                    "fingerprint": occurrence.fingerprint,
                },
            )
            if not case_rows:
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
                    q_error=ErrorEvidencePolicyV1().q_error(fault_count=1, success_count=0),
                    case_revision=1,
                    first_occurred_at=occurrence.occurred_at,
                    last_occurred_at=occurrence.occurred_at,
                )
                created = await fetch_all(
                    conn,
                    """
                    INSERT INTO debug_cases (
                        case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                        operation_kind, policy_version, comparison_key, state, fault_count,
                        success_count, q_error, case_revision, first_occurred_at,
                        last_occurred_at, resolved_at
                    ) VALUES (
                        %(case_id)s, %(tenant_id)s, %(fingerprint_version)s, %(fingerprint)s,
                        %(fault_class)s, %(operation_kind)s, %(policy_version)s,
                        %(comparison_key)s, %(state)s, %(fault_count)s, %(success_count)s,
                        %(q_error)s, %(case_revision)s, %(first_occurred_at)s,
                        %(last_occurred_at)s, NULL
                    )
                    RETURNING case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                              operation_kind, policy_version, comparison_key, state, fault_count,
                              success_count, q_error, case_revision, first_occurred_at,
                              last_occurred_at, resolved_at
                    """,
                    {
                        "case_id": case.case_id,
                        "tenant_id": case.tenant_id,
                        "fingerprint_version": case.fingerprint_version,
                        "fingerprint": case.fingerprint,
                        "fault_class": case.fault_class,
                        "operation_kind": case.operation_kind,
                        "policy_version": case.policy_version,
                        "comparison_key": Jsonb(case.comparison_key.model_dump(mode="json")),
                        "state": case.state,
                        "fault_count": case.fault_count,
                        "success_count": case.success_count,
                        "q_error": case.q_error,
                        "case_revision": case.case_revision,
                        "first_occurred_at": case.first_occurred_at,
                        "last_occurred_at": case.last_occurred_at,
                    },
                )
                if not created:
                    raise BrainValidationError("DebugCase creation was not durable")
                case = _debug_case_from_row(created[0])
            else:
                current = _debug_case_from_row(case_rows[0])
                if occurrence.occurred_at < current.last_occurred_at:
                    raise BrainValidationError("occurrence is out of order for DebugCase")
                if (
                    current.fault_class != occurrence.fault_class
                    or current.operation_kind != occurrence.operation_kind
                    or current.policy_version != occurrence.policy_version
                    or current.comparison_key != occurrence.comparison_key
                ):
                    raise BrainValidationError("fault occurrence conflicts with DebugCase identity")
                fault_count = current.fault_count + 1
                updated = await fetch_all(
                    conn,
                    """
                    UPDATE debug_cases
                    SET state = 'open', fault_count = %(fault_count)s,
                        q_error = %(q_error)s, case_revision = %(case_revision)s,
                        last_occurred_at = %(last_occurred_at)s, resolved_at = NULL
                    WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                      AND case_revision = %(expected_case_revision)s
                    RETURNING case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                              operation_kind, policy_version, comparison_key, state, fault_count,
                              success_count, q_error, case_revision, first_occurred_at,
                              last_occurred_at, resolved_at
                    """,
                    {
                        "fault_count": fault_count,
                        "q_error": ErrorEvidencePolicyV1().q_error(
                            fault_count=fault_count,
                            success_count=current.success_count,
                        ),
                        "case_revision": current.case_revision + 1,
                        "last_occurred_at": occurrence.occurred_at,
                        "case_id": current.case_id,
                        "tenant_id": current.tenant_id,
                        "expected_case_revision": current.case_revision,
                    },
                )
                if not updated:
                    raise BrainValidationError("stale DebugCase revision")
                case = _debug_case_from_row(updated[0])

            _ = await fetch_all(
                conn,
                """
                INSERT INTO debug_case_occurrences (
                    occurrence_id, case_id, tenant_id, producer_id, idempotency_key,
                    fingerprint_version, fingerprint, fault_class, operation_kind, fault_code,
                    policy_version, comparison_key, trace_id, graph_run_id, node_id, step_id,
                    occurred_at, canonical_digest
                ) VALUES (
                    %(occurrence_id)s, %(case_id)s, %(tenant_id)s, %(producer_id)s,
                    %(idempotency_key)s, %(fingerprint_version)s, %(fingerprint)s,
                    %(fault_class)s, %(operation_kind)s, %(fault_code)s, %(policy_version)s,
                    %(comparison_key)s, %(trace_id)s, %(graph_run_id)s, %(node_id)s,
                    %(step_id)s, %(occurred_at)s, %(canonical_digest)s
                )
                RETURNING occurrence_id
                """,
                {
                    "occurrence_id": occurrence.occurrence_id,
                    "case_id": case.case_id,
                    "tenant_id": occurrence.tenant_id,
                    "producer_id": occurrence.producer_id,
                    "idempotency_key": occurrence.idempotency_key,
                    "fingerprint_version": occurrence.fingerprint_version,
                    "fingerprint": occurrence.fingerprint,
                    "fault_class": occurrence.fault_class,
                    "operation_kind": occurrence.operation_kind,
                    "fault_code": occurrence.fault_code,
                    "policy_version": occurrence.policy_version,
                    "comparison_key": Jsonb(occurrence.comparison_key.model_dump(mode="json")),
                    "trace_id": occurrence.trace_id,
                    "graph_run_id": occurrence.graph_run_id,
                    "node_id": occurrence.node_id,
                    "step_id": occurrence.step_id,
                    "occurred_at": occurrence.occurred_at,
                    "canonical_digest": occurrence_digest,
                },
            )
            return case

    async def get_debug_case(self, *, tenant_id: str, case_id: UUID) -> DebugCase | None:
        """Read one tenant-owned DebugCase; PostgreSQL RLS enforces the same scope."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                       operation_kind, policy_version, comparison_key, state, fault_count,
                       success_count, q_error, case_revision, first_occurred_at,
                       last_occurred_at, resolved_at
                FROM debug_cases
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                """,
                {"case_id": case_id, "tenant_id": tenant_id},
            )
        return _debug_case_from_row(rows[0]) if rows else None

    async def get_debug_case_detail(
        self,
        *,
        tenant_id: str,
        case_id: UUID,
        history_limit: int,
    ) -> DebugCaseDetail | None:
        """Read one tenant-owned case and independently bounded closed history."""
        params = {
            "case_id": case_id,
            "tenant_id": tenant_id,
            "history_limit": history_limit,
        }
        async with await self.tenant_connection(tenant_id) as conn:
            case_rows = await fetch_all(
                conn,
                """
                SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                       operation_kind, policy_version, comparison_key, state, fault_count,
                       success_count, q_error, case_revision, first_occurred_at,
                       last_occurred_at, resolved_at
                FROM debug_cases
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                FOR SHARE
                """,
                params,
            )
            if not case_rows:
                return None
            occurrence_rows = await fetch_all(
                conn,
                """
                SELECT occurrence_id, fault_code, trace_id, graph_run_id, node_id,
                       step_id, occurred_at
                FROM debug_case_occurrences
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                ORDER BY occurred_at ASC, occurrence_id ASC
                LIMIT %(history_limit)s
                """,
                params,
            )
            mitigation_rows = await fetch_all(
                conn,
                """
                SELECT attempt_id, expected_case_revision, kind, attempted_at
                FROM debug_case_mitigations
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                ORDER BY attempted_at ASC, attempt_id ASC
                LIMIT %(history_limit)s
                """,
                params,
            )
            recovery_rows = await fetch_all(
                conn,
                """
                SELECT recovery_id, expected_case_revision, exposure_id, kind, verified_at
                FROM debug_case_recoveries
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                ORDER BY verified_at ASC, recovery_id ASC
                LIMIT %(history_limit)s
                """,
                params,
            )
            transition_rows = await fetch_all(
                conn,
                """
                SELECT transition_id, transition_kind, expected_case_revision,
                       trigger_occurrence_id, transitioned_at
                FROM debug_case_transitions
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                ORDER BY transitioned_at ASC, transition_id ASC
                LIMIT %(history_limit)s
                """,
                params,
            )

        return DebugCaseDetail(
            case=_debug_case_from_row(case_rows[0]),
            occurrences=tuple(
                DebugCaseOccurrenceView.model_validate(row) for row in occurrence_rows
            ),
            mitigations=tuple(
                DebugCaseMitigationView.model_validate(row) for row in mitigation_rows
            ),
            recoveries=tuple(DebugCaseRecoveryView.model_validate(row) for row in recovery_rows),
            transitions=tuple(
                DebugCaseTransitionView.model_validate(row) for row in transition_rows
            ),
        )

    async def report_recovery_evidence(
        self,
        *,
        tenant_id: str,
        evidence: RecoveryEvidence,
    ) -> DebugCase:
        """Store a unique comparable success and advance the aggregate with CAS."""
        evidence_digest = _canonical_digest(evidence)
        async with await self.tenant_connection(tenant_id) as conn:
            await _lock(conn, f"udb-recovery:{evidence.case_id}:{evidence.exposure_id}")
            existing = await fetch_all(
                conn,
                """
                SELECT canonical_digest FROM debug_case_recoveries
                WHERE case_id = %(case_id)s AND exposure_id = %(exposure_id)s
                """,
                {"case_id": evidence.case_id, "exposure_id": evidence.exposure_id},
            )
            if existing:
                if existing[0].get("canonical_digest") != evidence_digest:
                    raise BrainValidationError("conflicting recovery exposure id")
                case_rows = await fetch_all(
                    conn,
                    """
                    SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                           operation_kind, policy_version, comparison_key, state, fault_count,
                           success_count, q_error, case_revision, first_occurred_at,
                           last_occurred_at, resolved_at
                    FROM debug_cases
                    WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                    """,
                    {"case_id": evidence.case_id, "tenant_id": tenant_id},
                )
                if not case_rows:
                    raise BrainValidationError("stored recovery has no tenant-owned DebugCase")
                return _debug_case_from_row(case_rows[0])

            rows = await fetch_all(
                conn,
                """
                SELECT case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                       operation_kind, policy_version, comparison_key, state, fault_count,
                       success_count, q_error, case_revision, first_occurred_at,
                       last_occurred_at, resolved_at
                FROM debug_cases
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                FOR UPDATE
                """,
                {"case_id": evidence.case_id, "tenant_id": tenant_id},
            )
            if not rows:
                raise BrainValidationError("DebugCase not found for tenant")
            current = _debug_case_from_row(rows[0])
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
            updated = await fetch_all(
                conn,
                """
                UPDATE debug_cases
                SET success_count = %(success_count)s, q_error = %(q_error)s,
                    case_revision = %(case_revision)s
                WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s
                  AND case_revision = %(expected_case_revision)s
                RETURNING case_id, tenant_id, fingerprint_version, fingerprint, fault_class,
                          operation_kind, policy_version, comparison_key, state, fault_count,
                          success_count, q_error, case_revision, first_occurred_at,
                          last_occurred_at, resolved_at
                """,
                {
                    "success_count": success_count,
                    "q_error": ErrorEvidencePolicyV1().q_error(
                        fault_count=current.fault_count,
                        success_count=success_count,
                    ),
                    "case_revision": current.case_revision + 1,
                    "case_id": current.case_id,
                    "tenant_id": current.tenant_id,
                    "expected_case_revision": current.case_revision,
                },
            )
            if not updated:
                raise BrainValidationError("stale DebugCase revision")
            case = _debug_case_from_row(updated[0])
            _ = await fetch_all(
                conn,
                """
                INSERT INTO debug_case_recoveries (
                    recovery_id, case_id, tenant_id, policy_version, comparison_key,
                    expected_case_revision, exposure_id, kind, verified_at, canonical_digest
                ) VALUES (
                    %(recovery_id)s, %(case_id)s, %(tenant_id)s, %(policy_version)s,
                    %(comparison_key)s, %(expected_case_revision)s, %(exposure_id)s,
                    %(kind)s, %(verified_at)s, %(canonical_digest)s
                )
                RETURNING recovery_id
                """,
                {
                    "recovery_id": evidence.recovery_id,
                    "case_id": evidence.case_id,
                    "tenant_id": tenant_id,
                    "policy_version": evidence.policy_version,
                    "comparison_key": Jsonb(evidence.comparison_key.model_dump(mode="json")),
                    "expected_case_revision": evidence.expected_case_revision,
                    "exposure_id": evidence.exposure_id,
                    "kind": evidence.kind,
                    "verified_at": evidence.verified_at,
                    "canonical_digest": evidence_digest,
                },
            )
            return case

    async def report_mitigation_attempt(
        self, *, tenant_id: str, attempt: MitigationAttempt
    ) -> DebugCase:
        attempt_digest = _canonical_digest(attempt)
        async with await self.tenant_connection(tenant_id) as conn:
            await _lock(conn, f"udb-mitigation:{attempt.case_id}:{attempt.idempotency_key}")
            existing = await fetch_all(
                conn,
                "SELECT attempt_id, canonical_digest FROM debug_case_mitigations "
                "WHERE case_id = %(case_id)s AND idempotency_key = %(key)s",
                {"case_id": attempt.case_id, "key": attempt.idempotency_key},
            )
            if existing:
                if (
                    existing[0].get("attempt_id") != str(attempt.attempt_id)
                    or existing[0].get("canonical_digest") != attempt_digest
                ):
                    raise BrainValidationError("conflicting mitigation idempotency key")
                rows = await fetch_all(
                    conn,
                    "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s",
                    {"case_id": attempt.case_id, "tenant_id": tenant_id},
                )
                if not rows:
                    raise BrainValidationError("stored mitigation has no tenant-owned DebugCase")
                return _debug_case_from_row(rows[0])
            rows = await fetch_all(
                conn,
                "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s FOR UPDATE",
                {"case_id": attempt.case_id, "tenant_id": tenant_id},
            )
            if not rows:
                raise BrainValidationError("DebugCase not found for tenant")
            current = _debug_case_from_row(rows[0])
            if current.state != "open" or current.case_revision != attempt.expected_case_revision:
                raise BrainValidationError("stale or resolved DebugCase revision")
            _ = await fetch_all(
                conn,
                """
                INSERT INTO debug_case_mitigations (
                    attempt_id, case_id, tenant_id, expected_case_revision,
                    kind, idempotency_key, attempted_at, canonical_digest
                ) VALUES (
                    %(attempt_id)s, %(case_id)s, %(tenant_id)s, %(revision)s,
                    %(kind)s, %(key)s, %(attempted_at)s, %(canonical_digest)s
                ) RETURNING attempt_id
                """,
                {
                    "attempt_id": attempt.attempt_id,
                    "case_id": attempt.case_id,
                    "tenant_id": tenant_id,
                    "revision": attempt.expected_case_revision,
                    "kind": attempt.kind,
                    "key": attempt.idempotency_key,
                    "attempted_at": attempt.attempted_at,
                    "canonical_digest": attempt_digest,
                },
            )
            updated = await fetch_all(
                conn,
                "UPDATE debug_cases SET case_revision = case_revision + 1 WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s AND case_revision = %(revision)s RETURNING *",
                {
                    "case_id": attempt.case_id,
                    "tenant_id": tenant_id,
                    "revision": attempt.expected_case_revision,
                },
            )
            if not updated:
                raise BrainValidationError("stale DebugCase revision")
            return _debug_case_from_row(updated[0])

    async def resolve_debug_case(self, *, tenant_id: str, command: ResolveDebugCase) -> DebugCase:
        command_digest = _canonical_digest(command)
        async with await self.tenant_connection(tenant_id) as conn:
            await _lock(conn, f"udb-transition:{command.resolution_id}")
            existing = await fetch_all(
                conn,
                "SELECT transition_kind, case_id, canonical_digest "
                "FROM debug_case_transitions WHERE transition_id = %(id)s",
                {"id": command.resolution_id},
            )
            if existing:
                if (
                    existing[0].get("transition_kind") != "resolved"
                    or existing[0].get("case_id") != str(command.case_id)
                    or existing[0].get("canonical_digest") != command_digest
                ):
                    raise BrainValidationError("conflicting resolution id")
                rows = await fetch_all(
                    conn,
                    "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s",
                    {"case_id": command.case_id, "tenant_id": tenant_id},
                )
                if not rows:
                    raise BrainValidationError("stored resolution has no tenant-owned DebugCase")
                return _debug_case_from_row(rows[0])
            rows = await fetch_all(
                conn,
                "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s FOR UPDATE",
                {"case_id": command.case_id, "tenant_id": tenant_id},
            )
            if not rows:
                raise BrainValidationError("DebugCase not found for tenant")
            current = _debug_case_from_row(rows[0])
            if current.state != "open" or current.case_revision != command.expected_case_revision:
                raise BrainValidationError("stale or resolved DebugCase revision")
            if (
                current.success_count
                < ErrorEvidencePolicyV1(version=current.policy_version).minimum_success_count
            ):
                raise BrainValidationError(
                    "DebugCase requires comparable recovery before resolution"
                )
            _ = await fetch_all(
                conn,
                "INSERT INTO debug_case_transitions (transition_id, case_id, tenant_id, transition_kind, expected_case_revision, transitioned_at, canonical_digest) VALUES (%(id)s, %(case_id)s, %(tenant_id)s, 'resolved', %(revision)s, %(at)s, %(canonical_digest)s) RETURNING transition_id",
                {
                    "id": command.resolution_id,
                    "case_id": command.case_id,
                    "tenant_id": tenant_id,
                    "revision": command.expected_case_revision,
                    "at": command.resolved_at,
                    "canonical_digest": command_digest,
                },
            )
            updated = await fetch_all(
                conn,
                "UPDATE debug_cases SET state = 'resolved', resolved_at = %(at)s, case_revision = case_revision + 1 WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s AND case_revision = %(revision)s RETURNING *",
                {
                    "at": command.resolved_at,
                    "case_id": command.case_id,
                    "tenant_id": tenant_id,
                    "revision": command.expected_case_revision,
                },
            )
            if not updated:
                raise BrainValidationError("stale DebugCase revision")
            return _debug_case_from_row(updated[0])

    async def reopen_debug_case(self, *, tenant_id: str, command: ReopenDebugCase) -> DebugCase:
        command_digest = _canonical_digest(command)
        async with await self.tenant_connection(tenant_id) as conn:
            await _lock(conn, f"udb-transition:{command.reopen_id}")
            existing = await fetch_all(
                conn,
                "SELECT transition_kind, case_id, canonical_digest "
                "FROM debug_case_transitions WHERE transition_id = %(id)s",
                {"id": command.reopen_id},
            )
            if existing:
                if (
                    existing[0].get("transition_kind") != "reopened"
                    or existing[0].get("case_id") != str(command.case_id)
                    or existing[0].get("canonical_digest") != command_digest
                ):
                    raise BrainValidationError("conflicting reopen id")
                rows = await fetch_all(
                    conn,
                    "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s",
                    {"case_id": command.case_id, "tenant_id": tenant_id},
                )
                if not rows:
                    raise BrainValidationError("stored reopen has no tenant-owned DebugCase")
                return _debug_case_from_row(rows[0])
            trigger = await fetch_all(
                conn,
                "SELECT occurrence_id FROM debug_case_occurrences WHERE occurrence_id = %(occurrence_id)s AND case_id = %(case_id)s AND tenant_id = %(tenant_id)s",
                {
                    "occurrence_id": command.trigger_occurrence_id,
                    "case_id": command.case_id,
                    "tenant_id": tenant_id,
                },
            )
            if not trigger:
                raise BrainValidationError("reopen trigger occurrence not found for tenant case")
            rows = await fetch_all(
                conn,
                "SELECT * FROM debug_cases WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s FOR UPDATE",
                {"case_id": command.case_id, "tenant_id": tenant_id},
            )
            if not rows:
                raise BrainValidationError("DebugCase not found for tenant")
            current = _debug_case_from_row(rows[0])
            if (
                current.state != "resolved"
                or current.case_revision != command.expected_case_revision
            ):
                raise BrainValidationError("stale or open DebugCase revision")
            _ = await fetch_all(
                conn,
                "INSERT INTO debug_case_transitions (transition_id, case_id, tenant_id, transition_kind, expected_case_revision, trigger_occurrence_id, transitioned_at, canonical_digest) VALUES (%(id)s, %(case_id)s, %(tenant_id)s, 'reopened', %(revision)s, %(trigger)s, %(at)s, %(canonical_digest)s) RETURNING transition_id",
                {
                    "id": command.reopen_id,
                    "case_id": command.case_id,
                    "tenant_id": tenant_id,
                    "revision": command.expected_case_revision,
                    "trigger": command.trigger_occurrence_id,
                    "at": command.reopened_at,
                    "canonical_digest": command_digest,
                },
            )
            updated = await fetch_all(
                conn,
                "UPDATE debug_cases SET state = 'open', resolved_at = NULL, case_revision = case_revision + 1 WHERE case_id = %(case_id)s AND tenant_id = %(tenant_id)s AND case_revision = %(revision)s RETURNING *",
                {
                    "case_id": command.case_id,
                    "tenant_id": tenant_id,
                    "revision": command.expected_case_revision,
                },
            )
            if not updated:
                raise BrainValidationError("stale DebugCase revision")
            return _debug_case_from_row(updated[0])

    async def query_debug_cases(
        self,
        *,
        tenant_id: str,
        query: DebugCaseQuery,
        recurring_only: bool = False,
    ) -> list[DebugCase]:
        minimum = max(query.minimum_fault_count, 2 if recurring_only else 1)
        clauses = [
            "debug_cases.tenant_id = %(tenant_id)s",
            "debug_cases.fault_count >= %(minimum)s",
        ]
        if query.state is not None:
            clauses.append("debug_cases.state = %(state)s")
        occurrence_filters: list[str] = []
        if query.trace_id is not None:
            occurrence_filters.append("occurrence.trace_id = %(trace_id)s")
        if query.graph_run_id is not None:
            occurrence_filters.append("occurrence.graph_run_id = %(graph_run_id)s")
        if occurrence_filters:
            clauses.append(
                "EXISTS (SELECT 1 FROM debug_case_occurrences AS occurrence "
                "WHERE occurrence.case_id = debug_cases.case_id "
                "AND occurrence.tenant_id = debug_cases.tenant_id AND "
                + " AND ".join(occurrence_filters)
                + ")"
            )
        params = {
            "tenant_id": tenant_id,
            "minimum": minimum,
            "state": query.state,
            "trace_id": query.trace_id,
            "graph_run_id": query.graph_run_id,
            "limit": query.limit,
        }
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT debug_cases.* FROM debug_cases WHERE "
                + " AND ".join(clauses)
                + " ORDER BY debug_cases.fault_count DESC, "
                "debug_cases.last_occurred_at DESC LIMIT %(limit)s",
                params,
            )
        return [_debug_case_from_row(row) for row in rows]


__all__ = ["UdbMixin"]
