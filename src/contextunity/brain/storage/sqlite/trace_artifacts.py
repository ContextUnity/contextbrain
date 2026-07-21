"""Protected Execution Trace artifact persistence for SQLite."""

from __future__ import annotations

from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ProtectedExecutionTraceArtifactEnvelope,
)
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError

from .codecs import json_dumps, json_loads
from .connection import SqliteConnectionMixin


class TraceArtifactsMixin(SqliteConnectionMixin):
    """Tenant/project/attempt-scoped artifact CAS with no content deduplication."""

    async def reserve_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        lifecycle_profile_id: str,
        request_bytes: int,
    ) -> JsonDict:
        """Create or idempotently replay one protected request reservation."""
        identity = envelope.identity
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                """SELECT artifact_id, reservation_digest, revision FROM execution_trace_artifacts
                   WHERE tenant_id = ? AND project_id = ? AND graph_run_id = ?
                     AND provider_attempt_id = ? AND artifact_kind = ?""",
                (
                    identity.tenant_id,
                    identity.project_id,
                    str(identity.graph_run_id),
                    str(identity.provider_attempt_id),
                    identity.artifact_kind,
                ),
            ).fetchone()
            if existing is not None:
                if str(existing[1]) != envelope.content_digest:
                    raise BrainValidationError("conflicting trace artifact reservation")
                db.commit()
                return {
                    "artifact_id": str(existing[0]),
                    "content_digest": str(existing[1]),
                    "revision": int(existing[2]),
                    "outcome": "duplicate",
                }
            _ = db.execute(
                """INSERT INTO execution_trace_artifacts (
                       artifact_id, tenant_id, project_id, trace_id, graph_run_id,
                       invocation_id, provider_attempt_id, artifact_kind, content_schema,
                       capture_state, storage_state, lifecycle_profile_id, content_digest,
                       reservation_digest, protected_envelope, request_bytes, response_bytes,
                       revision
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'captured', 'hot', ?, ?, ?, ?, ?, 0, 1)""",
                (
                    str(envelope.artifact_id),
                    identity.tenant_id,
                    identity.project_id,
                    str(identity.trace_id),
                    str(identity.graph_run_id),
                    str(identity.invocation_id),
                    str(identity.provider_attempt_id),
                    identity.artifact_kind,
                    "contextunity.model-io-content/v1",
                    lifecycle_profile_id,
                    envelope.content_digest,
                    envelope.content_digest,
                    json_dumps(envelope.model_dump(mode="json")),
                    request_bytes,
                ),
            )
            db.commit()
        return {
            "artifact_id": str(envelope.artifact_id),
            "content_digest": envelope.content_digest,
            "revision": 1,
            "outcome": "created",
        }

    async def finalize_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        expected_revision: int,
        request_bytes: int,
        response_bytes: int,
    ) -> JsonDict:
        """Replace the request reservation with one protected terminal snapshot."""
        identity = envelope.identity
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                """SELECT content_digest, revision, storage_state
                   FROM execution_trace_artifacts
                   WHERE artifact_id = ? AND tenant_id = ? AND project_id = ?
                     AND graph_run_id = ? AND provider_attempt_id = ?""",
                (
                    str(envelope.artifact_id),
                    identity.tenant_id,
                    identity.project_id,
                    str(identity.graph_run_id),
                    str(identity.provider_attempt_id),
                ),
            ).fetchone()
            if existing is None:
                raise BrainValidationError("trace artifact reservation is missing")
            stored_digest = str(existing[0])
            revision = int(existing[1])
            if stored_digest == envelope.content_digest and revision > expected_revision:
                if revision != 2 or expected_revision != 1 or str(existing[2]) != "hot":
                    raise BrainValidationError("trace artifact finalize lifecycle conflict")
                db.commit()
                return {
                    "artifact_id": str(envelope.artifact_id),
                    "content_digest": stored_digest,
                    "storage_state": "hot",
                    "revision": revision,
                    "outcome": "duplicate",
                }
            if expected_revision != 1 or revision != expected_revision or str(existing[2]) != "hot":
                raise BrainValidationError("trace artifact finalize CAS conflict")
            _ = db.execute(
                """UPDATE execution_trace_artifacts
                   SET content_digest = ?, protected_envelope = ?, request_bytes = ?,
                       response_bytes = ?, revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE artifact_id = ? AND revision = ?""",
                (
                    envelope.content_digest,
                    json_dumps(envelope.model_dump(mode="json")),
                    request_bytes,
                    response_bytes,
                    str(envelope.artifact_id),
                    expected_revision,
                ),
            )
            db.commit()
        return {
            "artifact_id": str(envelope.artifact_id),
            "content_digest": envelope.content_digest,
            "storage_state": "hot",
            "revision": expected_revision + 1,
            "outcome": "finalized",
        }

    async def get_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
    ) -> JsonDict | None:
        """Read one protected envelope only through exact tenant/project scope."""
        with self._get_connection() as db:
            row = db.execute(
                """SELECT artifact_id, tenant_id, project_id, trace_id, graph_run_id,
                          invocation_id, provider_attempt_id, artifact_kind, content_schema,
                          capture_state, storage_state, lifecycle_profile_id, content_digest,
                          reservation_digest, protected_envelope, archive_receipt,
                          request_bytes, response_bytes, revision, created_at, updated_at,
                          purged_at
                   FROM execution_trace_artifacts
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?""",
                (tenant_id, project_id, artifact_id),
            ).fetchone()
        if row is None:
            return None
        envelope = json_loads(str(row[14])) if row[14] is not None else None
        archive_receipt = json_loads(str(row[15])) if row[15] is not None else None
        return {
            "artifact_id": str(row[0]),
            "tenant_id": str(row[1]),
            "project_id": str(row[2]),
            "trace_id": str(row[3]),
            "graph_run_id": str(row[4]),
            "invocation_id": str(row[5]),
            "provider_attempt_id": str(row[6]),
            "artifact_kind": str(row[7]),
            "content_schema": str(row[8]),
            "capture_state": str(row[9]),
            "storage_state": str(row[10]),
            "lifecycle_profile_id": str(row[11]),
            "content_digest": str(row[12]),
            "reservation_digest": str(row[13]),
            "protected_envelope": envelope,
            "archive_receipt": archive_receipt,
            "request_bytes": int(row[16]),
            "response_bytes": int(row[17]),
            "revision": int(row[18]),
            "created_at": str(row[19]),
            "updated_at": str(row[20]),
            "purged_at": None if row[21] is None else str(row[21]),
        }

    async def begin_archive_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Claim one finalized hot artifact for cold offload by CAS."""
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'archiving', revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND revision = ? AND storage_state = 'hot'""",
                (tenant_id, project_id, artifact_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact archive CAS conflict")
            db.commit()
        return {
            "artifact_id": artifact_id,
            "storage_state": "archiving",
            "revision": expected_revision + 1,
        }

    async def archive_execution_trace_artifact(
        self,
        *,
        receipt: ExecutionTraceArtifactArchiveReceipt,
        expected_revision: int,
    ) -> JsonDict:
        """Move one exact hot envelope to a URI-free cold receipt by CAS."""
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = NULL, archive_receipt = ?,
                       storage_state = 'cold', revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND content_digest = ? AND revision = ? AND storage_state = 'archiving'""",
                (
                    json_dumps(receipt.model_dump(mode="json")),
                    receipt.identity.tenant_id,
                    receipt.identity.project_id,
                    str(receipt.artifact_id),
                    receipt.content_digest,
                    expected_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact archive CAS conflict")
            db.commit()
        return {
            "artifact_id": str(receipt.artifact_id),
            "storage_state": "cold",
            "revision": expected_revision + 1,
        }

    async def begin_restore_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Claim one cold artifact for restore by CAS."""
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'restoring', revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND revision = ? AND storage_state = 'cold'""",
                (tenant_id, project_id, artifact_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact restore CAS conflict")
            db.commit()
        return {
            "artifact_id": artifact_id,
            "storage_state": "restoring",
            "revision": expected_revision + 1,
        }

    async def stage_restore_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        expected_revision: int,
    ) -> JsonDict:
        """Persist identity-verified ciphertext while retaining the restore claim."""
        identity = envelope.identity
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = ?, revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND graph_run_id = ? AND provider_attempt_id = ?
                     AND content_digest = ? AND revision = ?
                     AND storage_state = 'restoring' AND protected_envelope IS NULL""",
                (
                    json_dumps(envelope.model_dump(mode="json")),
                    identity.tenant_id,
                    identity.project_id,
                    str(envelope.artifact_id),
                    str(identity.graph_run_id),
                    str(identity.provider_attempt_id),
                    envelope.content_digest,
                    expected_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact restore staging CAS conflict")
            db.commit()
        return {
            "artifact_id": str(envelope.artifact_id),
            "storage_state": "restoring",
            "revision": expected_revision + 1,
        }

    async def complete_restore_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        """Commit hot authority and clear the cold receipt by CAS."""
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET archive_receipt = NULL, storage_state = 'hot',
                       revision = revision + 1, updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND revision = ? AND storage_state = 'restoring'
                     AND protected_envelope IS NOT NULL""",
                (tenant_id, project_id, artifact_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact restore completion CAS conflict")
            db.commit()
        return {
            "artifact_id": artifact_id,
            "storage_state": "hot",
            "revision": expected_revision + 1,
        }

    async def begin_purge_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
        legal_hold: bool,
    ) -> JsonDict:
        """Claim one stable hot or cold artifact for resumable purge."""
        if legal_hold:
            raise BrainValidationError("trace artifact is under legal hold")
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'purging', revision = revision + 1,
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND revision = ? AND storage_state IN ('hot', 'cold')""",
                (tenant_id, project_id, artifact_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact purge CAS conflict")
            db.commit()
        return {
            "artifact_id": artifact_id,
            "storage_state": "purging",
            "revision": expected_revision + 1,
        }

    async def purge_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
        legal_hold: bool,
    ) -> JsonDict:
        """Remove ciphertext while preserving a minimal trace-bound tombstone."""
        if legal_hold:
            raise BrainValidationError("trace artifact is under legal hold")
        with self._get_connection() as db:
            cursor = db.execute(
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = NULL, archive_receipt = NULL,
                       storage_state = 'purged',
                       revision = revision + 1, purged_at = datetime('now'),
                       updated_at = datetime('now')
                   WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?
                     AND revision = ? AND storage_state = 'purging'""",
                (tenant_id, project_id, artifact_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise BrainValidationError("trace artifact purge CAS conflict")
            db.commit()
        return {
            "artifact_id": artifact_id,
            "storage_state": "purged",
            "revision": expected_revision + 1,
        }


__all__ = ["TraceArtifactsMixin"]
