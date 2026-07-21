"""Protected Execution Trace artifact persistence for PostgreSQL."""

from __future__ import annotations

from abc import ABC

from contextunity.core.narrowing import as_int
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ProtectedExecutionTraceArtifactEnvelope,
)
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.exceptions import BrainValidationError

from .base import PostgresStoreBase
from .helpers import Json, fetch_all


class TraceArtifactsMixin(PostgresStoreBase, ABC):
    """RLS-protected artifact CAS matching the SQLite contract."""

    async def reserve_execution_trace_artifact(
        self,
        *,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        lifecycle_profile_id: str,
        request_bytes: int,
    ) -> JsonDict:
        identity = envelope.identity
        async with await self.tenant_connection(identity.tenant_id) as conn:
            existing = await fetch_all(
                conn,
                """SELECT artifact_id, reservation_digest, revision
                   FROM execution_trace_artifacts
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND graph_run_id = %(graph_run_id)s
                     AND provider_attempt_id = %(provider_attempt_id)s
                     AND artifact_kind = %(artifact_kind)s
                   FOR UPDATE""",
                {
                    "tenant_id": identity.tenant_id,
                    "project_id": identity.project_id,
                    "graph_run_id": str(identity.graph_run_id),
                    "provider_attempt_id": str(identity.provider_attempt_id),
                    "artifact_kind": identity.artifact_kind,
                },
            )
            if existing:
                if str(existing[0].get("reservation_digest", "")) != envelope.content_digest:
                    raise BrainValidationError("conflicting trace artifact reservation")
                return {
                    "artifact_id": str(existing[0].get("artifact_id", "")),
                    "content_digest": envelope.content_digest,
                    "revision": as_int(existing[0].get("revision")),
                    "outcome": "duplicate",
                }
            inserted = await fetch_all(
                conn,
                """INSERT INTO execution_trace_artifacts (
                       artifact_id, tenant_id, project_id, trace_id, graph_run_id,
                       invocation_id, provider_attempt_id, artifact_kind, content_schema,
                       capture_state, storage_state, lifecycle_profile_id, content_digest,
                       reservation_digest, protected_envelope, request_bytes,
                       response_bytes, revision
                   ) VALUES (
                       %(artifact_id)s, %(tenant_id)s, %(project_id)s, %(trace_id)s,
                       %(graph_run_id)s, %(invocation_id)s, %(provider_attempt_id)s,
                       %(artifact_kind)s, 'contextunity.model-io-content/v1',
                       'captured', 'hot', %(lifecycle_profile_id)s, %(content_digest)s,
                       %(content_digest)s, %(protected_envelope)s, %(request_bytes)s, 0, 1
                   )
                   ON CONFLICT (tenant_id, project_id, graph_run_id,
                                provider_attempt_id, artifact_kind) DO NOTHING
                   RETURNING artifact_id""",
                {
                    "artifact_id": str(envelope.artifact_id),
                    "tenant_id": identity.tenant_id,
                    "project_id": identity.project_id,
                    "trace_id": str(identity.trace_id),
                    "graph_run_id": str(identity.graph_run_id),
                    "invocation_id": str(identity.invocation_id),
                    "provider_attempt_id": str(identity.provider_attempt_id),
                    "artifact_kind": identity.artifact_kind,
                    "lifecycle_profile_id": lifecycle_profile_id,
                    "content_digest": envelope.content_digest,
                    "protected_envelope": Json(envelope.model_dump(mode="json")),
                    "request_bytes": request_bytes,
                },
            )
        if not inserted:
            return await self.reserve_execution_trace_artifact(
                envelope=envelope,
                lifecycle_profile_id=lifecycle_profile_id,
                request_bytes=request_bytes,
            )
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
        identity = envelope.identity
        async with await self.tenant_connection(identity.tenant_id) as conn:
            existing = await fetch_all(
                conn,
                """SELECT content_digest, revision, storage_state
                   FROM execution_trace_artifacts
                   WHERE artifact_id = %(artifact_id)s AND tenant_id = %(tenant_id)s
                     AND project_id = %(project_id)s AND graph_run_id = %(graph_run_id)s
                     AND provider_attempt_id = %(provider_attempt_id)s
                   FOR UPDATE""",
                {
                    "artifact_id": str(envelope.artifact_id),
                    "tenant_id": identity.tenant_id,
                    "project_id": identity.project_id,
                    "graph_run_id": str(identity.graph_run_id),
                    "provider_attempt_id": str(identity.provider_attempt_id),
                },
            )
            if not existing:
                raise BrainValidationError("trace artifact reservation is missing")
            row = existing[0]
            revision = as_int(row.get("revision"))
            if (
                str(row.get("content_digest", "")) == envelope.content_digest
                and revision > expected_revision
            ):
                if (
                    revision != 2
                    or expected_revision != 1
                    or str(row.get("storage_state", "")) != "hot"
                ):
                    raise BrainValidationError("trace artifact finalize lifecycle conflict")
                return {
                    "artifact_id": str(envelope.artifact_id),
                    "content_digest": envelope.content_digest,
                    "storage_state": "hot",
                    "revision": revision,
                    "outcome": "duplicate",
                }
            if (
                expected_revision != 1
                or revision != expected_revision
                or str(row.get("storage_state", "")) != "hot"
            ):
                raise BrainValidationError("trace artifact finalize CAS conflict")
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET content_digest = %(content_digest)s,
                       protected_envelope = %(protected_envelope)s,
                       request_bytes = %(request_bytes)s,
                       response_bytes = %(response_bytes)s,
                       revision = revision + 1, updated_at = now()
                   WHERE artifact_id = %(artifact_id)s AND revision = %(expected_revision)s
                   RETURNING revision""",
                {
                    "content_digest": envelope.content_digest,
                    "protected_envelope": Json(envelope.model_dump(mode="json")),
                    "request_bytes": request_bytes,
                    "response_bytes": response_bytes,
                    "artifact_id": str(envelope.artifact_id),
                    "expected_revision": expected_revision,
                },
            )
            if not updated:
                raise BrainValidationError("trace artifact finalize CAS conflict")
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
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                """SELECT artifact_id, tenant_id, project_id, trace_id, graph_run_id,
                          invocation_id, provider_attempt_id, artifact_kind, content_schema,
                          capture_state, storage_state, lifecycle_profile_id, content_digest,
                          reservation_digest, protected_envelope, archive_receipt,
                          request_bytes, response_bytes, revision, created_at, updated_at,
                          purged_at
                   FROM execution_trace_artifacts
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                },
            )
        if not rows:
            return None
        row = rows[0]
        envelope = row.get("protected_envelope")
        if envelope is not None and not is_json_dict(envelope):
            raise BrainValidationError("stored trace artifact envelope is malformed")
        return JsonDict({key: value for key, value in row.items()})

    async def begin_archive_execution_trace_artifact(
        self,
        *,
        tenant_id: str,
        project_id: str,
        artifact_id: str,
        expected_revision: int,
    ) -> JsonDict:
        async with await self.tenant_connection(tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'archiving', revision = revision + 1,
                       updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND revision = %(expected_revision)s AND storage_state = 'hot'
                   RETURNING revision""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact archive CAS conflict")
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
        identity = receipt.identity
        async with await self.tenant_connection(identity.tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = NULL,
                       archive_receipt = %(archive_receipt)s,
                       storage_state = 'cold', revision = revision + 1,
                       updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND content_digest = %(content_digest)s
                     AND revision = %(expected_revision)s
                     AND storage_state = 'archiving'
                   RETURNING revision""",
                {
                    "archive_receipt": Json(receipt.model_dump(mode="json")),
                    "tenant_id": identity.tenant_id,
                    "project_id": identity.project_id,
                    "artifact_id": str(receipt.artifact_id),
                    "content_digest": receipt.content_digest,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact archive CAS conflict")
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
        async with await self.tenant_connection(tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'restoring', revision = revision + 1,
                       updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND revision = %(expected_revision)s AND storage_state = 'cold'
                   RETURNING revision""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact restore CAS conflict")
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
        identity = envelope.identity
        async with await self.tenant_connection(identity.tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = %(protected_envelope)s,
                       revision = revision + 1, updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND graph_run_id = %(graph_run_id)s
                     AND provider_attempt_id = %(provider_attempt_id)s
                     AND content_digest = %(content_digest)s
                     AND revision = %(expected_revision)s
                     AND storage_state = 'restoring' AND protected_envelope IS NULL
                   RETURNING revision""",
                {
                    "protected_envelope": Json(envelope.model_dump(mode="json")),
                    "tenant_id": identity.tenant_id,
                    "project_id": identity.project_id,
                    "artifact_id": str(envelope.artifact_id),
                    "graph_run_id": str(identity.graph_run_id),
                    "provider_attempt_id": str(identity.provider_attempt_id),
                    "content_digest": envelope.content_digest,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact restore staging CAS conflict")
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
        async with await self.tenant_connection(tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET archive_receipt = NULL, storage_state = 'hot',
                       revision = revision + 1, updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND revision = %(expected_revision)s
                     AND storage_state = 'restoring'
                     AND protected_envelope IS NOT NULL
                   RETURNING revision""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact restore completion CAS conflict")
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
        if legal_hold:
            raise BrainValidationError("trace artifact is under legal hold")
        async with await self.tenant_connection(tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET storage_state = 'purging', revision = revision + 1,
                       updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND revision = %(expected_revision)s
                     AND storage_state IN ('hot', 'cold')
                   RETURNING revision""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact purge CAS conflict")
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
        if legal_hold:
            raise BrainValidationError("trace artifact is under legal hold")
        async with await self.tenant_connection(tenant_id) as conn:
            updated = await fetch_all(
                conn,
                """UPDATE execution_trace_artifacts
                   SET protected_envelope = NULL, archive_receipt = NULL,
                       storage_state = 'purged',
                       revision = revision + 1, purged_at = now(), updated_at = now()
                   WHERE tenant_id = %(tenant_id)s AND project_id = %(project_id)s
                     AND artifact_id = %(artifact_id)s
                     AND revision = %(expected_revision)s AND storage_state = 'purging'
                   RETURNING artifact_id""",
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "expected_revision": expected_revision,
                },
            )
            if len(updated) != 1:
                raise BrainValidationError("trace artifact purge CAS conflict")
        return {
            "artifact_id": artifact_id,
            "storage_state": "purged",
            "revision": expected_revision + 1,
        }


__all__ = ["TraceArtifactsMixin"]
