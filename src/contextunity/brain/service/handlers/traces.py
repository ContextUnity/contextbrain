"""Trace handlers - agent execution traces."""

from __future__ import annotations

from base64 import b64decode, b64encode
from binascii import Error as BinasciiError
from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.exceptions import SecurityError
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.narrowing import as_int
from contextunity.core.parsing import json_dumps
from contextunity.core.permissions import Permissions
from contextunity.core.sdk.execution_trace_artifacts import (
    ArtifactStorageState,
    ExecutionTraceArtifactArchiveReceipt,
    ExecutionTraceArtifactRef,
    ModelIOContent,
    ProtectedExecutionTraceArtifactEnvelope,
    ProtectExecutionTraceArtifactRequest,
    UnprotectExecutionTraceArtifactRequest,
)
from contextunity.core.tokens import ContextToken, PlatformBound, ProjectBound

from ...core.exceptions import BrainValidationError
from ...payloads import (
    ArchiveExecutionTraceArtifactPayload,
    FinalizeExecutionTraceArtifactPayload,
    GetExecutionTraceArtifactPayload,
    GetTracesPayload,
    LogTracePayload,
    PurgeExecutionTraceArtifactPayload,
    ReserveExecutionTraceArtifactPayload,
    RestoreExecutionTraceArtifactPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
    validate_user_access,
)

logger = get_contextunit_logger(__name__)


def _validate_project_binding(
    token: ContextToken | None,
    project_id: str,
    *,
    allow_platform: bool = False,
) -> None:
    binding = token.project_binding if token is not None else None
    if isinstance(binding, ProjectBound) and binding.project_id == project_id:
        return
    if allow_platform and isinstance(binding, PlatformBound):
        return
    raise SecurityError(msg="Trace artifact project binding denied")


def _validate_artifact_lifecycle_permission(token: ContextToken | None) -> None:
    if token is None or not token.has_permission(Permissions.TRACE_ARTIFACT_LIFECYCLE):
        raise SecurityError(msg="Trace artifact lifecycle permission denied")


def _protected_plaintext(value: object) -> str:
    return b64encode(json_dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8")).decode(
        "ascii"
    )


class TraceHandlersMixin(BrainHandlerBase):
    """Mixin for agent traces and their separately protected artifacts."""

    @grpc_error_handler
    async def LogTrace(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Log an agent execution trace."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = LogTracePayload.model_validate(unit.payload or {})
        if params.terminal_trace is not None:
            terminal = params.terminal_trace
            validate_tenant_access(
                token,
                terminal.tenant_id,
                context,
                operation="write",
                record_kind="execution_trace",
            )
            validate_user_access(token, terminal.user_id, context)
            # The digest is over the exact Router-authored canonical object.
            # Pydantic defaults (for example an absent registration_hash=None)
            # must not be injected before storage verifies that digest.
            terminal_wire = terminal.model_dump(mode="json", exclude_unset=True)
            receipt = await self.storage.finalize_execution_trace(
                terminal_trace=terminal_wire,
            )
            return make_response(payload=receipt, parent_unit=unit)

        if params.tenant_id is None or params.agent_id is None:
            raise ValueError("legacy trace identity is missing")
        validate_tenant_access(
            token, params.tenant_id, context, operation="write", record_kind="trace"
        )
        validate_user_access(token, params.user_id, context)

        user_id = params.user_id

        # Provenance: use caller-provided chain as-is (storage is infra, not data journey)
        provenance = list(params.provenance or [])

        trace_id = await self.storage.log_trace(
            tenant_id=params.tenant_id,
            agent_id=params.agent_id,
            session_id=params.session_id,
            user_id=user_id,
            graph_name=params.graph_name,
            tool_calls=params.tool_calls,
            token_usage=params.token_usage,
            timing_ms=params.timing_ms,
            security_flags=params.security_flags,
            metadata=params.metadata,
            provenance=provenance,
        )

        logger.info(
            "Trace logged: %s agent=%s graph=%s provenance=%s",
            trace_id,
            params.agent_id,
            params.graph_name,
            provenance,
        )

        return make_response(
            payload={"id": trace_id, "success": True},
            parent_unit=unit,
        )

    def _artifact_protector_and_profile(self, profile_id: str):
        settings = self.trace_artifact_settings
        if settings.protector != "shield_rpc" or self.trace_artifact_protector is None:
            raise BrainValidationError("trace artifact protector is unavailable")
        try:
            profile = settings.lifecycle_profile(profile_id)
        except ValueError as exc:
            raise BrainValidationError("trace artifact lifecycle profile is not allowed") from exc
        return self.trace_artifact_protector, profile

    @grpc_error_handler
    async def ReserveExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Protect and reserve provider-bound request content before model egress."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ReserveExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(
            token,
            params.identity.tenant_id,
            context,
            operation="write",
            record_kind="execution_trace_artifact",
        )
        _validate_project_binding(token, params.identity.project_id)
        protector, _profile = self._artifact_protector_and_profile(params.lifecycle_profile_id)
        request_bytes = sum(part.byte_count for part in params.request_parts)
        settings = self.trace_artifact_settings
        if (
            len(params.request_parts) > settings.max_parts
            or any(part.byte_count > settings.max_part_bytes for part in params.request_parts)
            or request_bytes > settings.max_total_bytes
        ):
            raise BrainValidationError("trace artifact request exceeds the C0 payload ceiling")
        protection_request = ProtectExecutionTraceArtifactRequest(
            purpose="execution_trace_artifact/model_io",
            identity=params.identity,
            artifact_id=params.artifact_id,
            plaintext_b64=_protected_plaintext(params.model_dump(mode="json")),
        )
        envelope = await protector.protect(protection_request)
        receipt = await self.storage.reserve_execution_trace_artifact(
            envelope=envelope,
            lifecycle_profile_id=params.lifecycle_profile_id,
            request_bytes=request_bytes,
        )
        return make_response(payload=receipt, parent_unit=unit)

    @grpc_error_handler
    async def FinalizeExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Finalize one visible model response through protected CAS storage."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = FinalizeExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(
            token,
            params.identity.tenant_id,
            context,
            operation="write",
            record_kind="execution_trace_artifact",
        )
        _validate_project_binding(token, params.identity.project_id)
        row = await self.storage.get_execution_trace_artifact(
            tenant_id=params.identity.tenant_id,
            project_id=params.identity.project_id,
            artifact_id=str(params.artifact_id),
        )
        if row is None or row.get("protected_envelope") is None:
            raise BrainValidationError("trace artifact reservation is missing")
        revision = as_int(row.get("revision"))
        if str(row.get("storage_state", "")) != "hot" or revision not in {1, 2}:
            raise BrainValidationError("trace artifact finalize lifecycle conflict")
        if revision == 2 and params.expected_revision != 1:
            raise BrainValidationError("trace artifact finalize CAS conflict")
        lifecycle_profile_id = str(row.get("lifecycle_profile_id", ""))
        protector, _profile = self._artifact_protector_and_profile(lifecycle_profile_id)
        envelope = ProtectedExecutionTraceArtifactEnvelope.model_validate(row["protected_envelope"])
        recovered = await protector.unprotect(
            UnprotectExecutionTraceArtifactRequest(envelope=envelope)
        )
        try:
            protected_wire = b64decode(recovered.plaintext_b64, validate=True)
            if revision == 1:
                reserve = ReserveExecutionTraceArtifactPayload.model_validate_json(protected_wire)
                if reserve.identity != params.identity or reserve.artifact_id != params.artifact_id:
                    raise BrainValidationError("trace artifact finalize binding mismatch")
                request_parts = reserve.request_parts
                content = ModelIOContent(
                    request_parts=request_parts,
                    response_parts=params.response_parts,
                    provider_status=params.provider_status,
                )
            else:
                content = ModelIOContent.model_validate_json(protected_wire)
                request_parts = content.request_parts
                if (
                    content.response_parts != params.response_parts
                    or content.provider_status != params.provider_status
                ):
                    raise BrainValidationError("conflicting trace artifact finalization")
        except (BinasciiError, ValueError) as exc:
            raise BrainValidationError("protected trace artifact content is malformed") from exc
        response_bytes = sum(part.byte_count for part in params.response_parts)
        settings = self.trace_artifact_settings
        if (
            len(params.response_parts) + len(request_parts) > settings.max_parts
            or any(part.byte_count > settings.max_part_bytes for part in params.response_parts)
            or sum(part.byte_count for part in request_parts) + response_bytes
            > settings.max_total_bytes
        ):
            raise BrainValidationError("trace artifact response exceeds the C0 payload ceiling")
        terminal_envelope = envelope
        if revision == 1:
            terminal_envelope = await protector.protect(
                ProtectExecutionTraceArtifactRequest(
                    purpose="execution_trace_artifact/model_io",
                    identity=params.identity,
                    artifact_id=params.artifact_id,
                    plaintext_b64=_protected_plaintext(content.model_dump(mode="json")),
                )
            )
        receipt = await self.storage.finalize_execution_trace_artifact(
            envelope=terminal_envelope,
            expected_revision=params.expected_revision,
            request_bytes=sum(part.byte_count for part in request_parts),
            response_bytes=response_bytes,
        )
        storage_state_raw = receipt.get("storage_state")
        if not isinstance(storage_state_raw, str) or storage_state_raw != "hot":
            raise BrainValidationError("trace artifact finalize returned invalid state")
        storage_state: ArtifactStorageState = storage_state_raw
        artifact_ref = ExecutionTraceArtifactRef(
            artifact_id=params.artifact_id,
            identity=params.identity,
            capture_state="captured",
            storage_state=storage_state,
            content_digest=terminal_envelope.content_digest,
            request_bytes=sum(part.byte_count for part in request_parts),
            response_bytes=response_bytes,
        )
        return make_response(
            payload={**receipt, "artifact_ref": artifact_ref.model_dump(mode="json")},
            parent_unit=unit,
        )

    @grpc_error_handler
    async def GetExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Authorize, unprotect and return one bounded model-attempt detail."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.TRACE_READ)
        if token is None or not token.has_permission(Permissions.TRACE_ARTIFACT_READ):
            raise SecurityError(msg="Trace artifact protected-read permission denied")
        params = GetExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        _validate_project_binding(token, params.project_id, allow_platform=True)
        row = await self.storage.get_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
        )
        if row is None:
            raise BrainValidationError("trace artifact was not found")
        safe = {
            key: value
            for key, value in row.items()
            if key not in {"protected_envelope", "archive_receipt", "reservation_digest"}
        }
        if row.get("storage_state") != "hot":
            return make_response(payload=safe, parent_unit=unit)
        protected = row.get("protected_envelope")
        if protected is None:
            raise BrainValidationError("trace artifact protected payload is unavailable")
        protector, _profile = self._artifact_protector_and_profile(
            str(row.get("lifecycle_profile_id", ""))
        )
        envelope = ProtectedExecutionTraceArtifactEnvelope.model_validate(protected)
        recovered = await protector.unprotect(
            UnprotectExecutionTraceArtifactRequest(envelope=envelope)
        )
        try:
            plaintext = b64decode(recovered.plaintext_b64, validate=True)
            if as_int(row.get("revision")) == 1:
                reservation = ReserveExecutionTraceArtifactPayload.model_validate_json(plaintext)
                detail = {
                    "reservation": {
                        "request_parts": [
                            part.model_dump(mode="json") for part in reservation.request_parts
                        ]
                    }
                }
            else:
                content = ModelIOContent.model_validate_json(plaintext)
                detail = {"content": content.model_dump(mode="json")}
        except (BinasciiError, ValueError) as exc:
            raise BrainValidationError("protected trace artifact content is malformed") from exc
        return make_response(payload={**safe, **detail}, parent_unit=unit)

    @grpc_error_handler
    async def ArchiveExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Offload one finalized artifact through Worker and commit cold state by CAS."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        _validate_artifact_lifecycle_permission(token)
        params = ArchiveExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(
            token,
            params.tenant_id,
            context,
            operation="write",
            record_kind="execution_trace_artifact",
        )
        _validate_project_binding(token, params.project_id)
        _protector, profile = self._artifact_protector_and_profile(params.lifecycle_profile_id)
        if profile.offload_profile_id is None or self.trace_artifact_archive is None:
            raise BrainValidationError("trace artifact archive profile is unavailable")
        row = await self.storage.get_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
        )
        if row is None or row.get("lifecycle_profile_id") != params.lifecycle_profile_id:
            raise BrainValidationError("trace artifact archive binding mismatch")
        state = str(row.get("storage_state", ""))
        revision = as_int(row.get("revision"))
        if row.get("content_digest") == row.get("reservation_digest"):
            raise BrainValidationError("trace artifact is not finalized")
        if state == "cold" and revision == params.expected_revision + 2:
            receipt = ExecutionTraceArtifactArchiveReceipt.model_validate(
                row.get("archive_receipt")
            )
            if (
                receipt.identity.tenant_id != params.tenant_id
                or receipt.identity.project_id != params.project_id
                or receipt.artifact_id != params.artifact_id
                or receipt.content_digest != row.get("content_digest")
                or receipt.offload_profile_id != profile.offload_profile_id
                or receipt.source_revision != params.expected_revision
            ):
                raise BrainValidationError("trace artifact archive receipt mismatch")
            return make_response(
                payload={
                    "artifact_id": str(params.artifact_id),
                    "storage_state": "cold",
                    "revision": revision,
                },
                parent_unit=unit,
            )
        if state == "hot" and revision == params.expected_revision:
            begun = await self.storage.begin_archive_execution_trace_artifact(
                tenant_id=params.tenant_id,
                project_id=params.project_id,
                artifact_id=str(params.artifact_id),
                expected_revision=params.expected_revision,
            )
            revision = as_int(begun.get("revision"))
        elif state != "archiving" or revision != params.expected_revision + 1:
            raise BrainValidationError("trace artifact archive CAS conflict")
        protected = row.get("protected_envelope")
        if protected is None:
            raise BrainValidationError("trace artifact archive payload is unavailable")
        envelope = ProtectedExecutionTraceArtifactEnvelope.model_validate(protected)
        prior_receipt = row.get("archive_receipt")
        if prior_receipt is not None:
            receipt = ExecutionTraceArtifactArchiveReceipt.model_validate(prior_receipt)
            if (
                receipt.identity != envelope.identity
                or receipt.artifact_id != envelope.artifact_id
                or receipt.content_digest != envelope.content_digest
                or receipt.offload_profile_id != profile.offload_profile_id
            ):
                raise BrainValidationError("trace artifact archive receipt mismatch")
        else:
            receipt = await self.trace_artifact_archive.archive(
                envelope,
                offload_profile_id=profile.offload_profile_id,
                source_revision=params.expected_revision,
            )
        archived = await self.storage.archive_execution_trace_artifact(
            receipt=receipt,
            expected_revision=revision,
        )
        return make_response(payload=archived, parent_unit=unit)

    @grpc_error_handler
    async def RestoreExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Restore one cold artifact through Worker with identity/digest CAS."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        _validate_artifact_lifecycle_permission(token)
        params = RestoreExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(
            token,
            params.tenant_id,
            context,
            operation="write",
            record_kind="execution_trace_artifact",
        )
        _validate_project_binding(token, params.project_id)
        _protector, profile = self._artifact_protector_and_profile(params.lifecycle_profile_id)
        if profile.offload_profile_id is None or self.trace_artifact_archive is None:
            raise BrainValidationError("trace artifact archive profile is unavailable")
        row = await self.storage.get_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
        )
        if row is None or row.get("lifecycle_profile_id") != params.lifecycle_profile_id:
            raise BrainValidationError("trace artifact restore binding mismatch")
        state = str(row.get("storage_state", ""))
        revision = as_int(row.get("revision"))
        if state == "hot" and revision == params.expected_revision + 3:
            if row.get("archive_receipt") is not None:
                raise BrainValidationError("trace artifact restore authority conflict")
            return make_response(
                payload={
                    "artifact_id": str(params.artifact_id),
                    "storage_state": "hot",
                    "revision": revision,
                },
                parent_unit=unit,
            )
        if state == "cold" and revision == params.expected_revision:
            begun = await self.storage.begin_restore_execution_trace_artifact(
                tenant_id=params.tenant_id,
                project_id=params.project_id,
                artifact_id=str(params.artifact_id),
                expected_revision=params.expected_revision,
            )
            revision = as_int(begun.get("revision"))
        elif state != "restoring" or revision not in {
            params.expected_revision + 1,
            params.expected_revision + 2,
        }:
            raise BrainValidationError("trace artifact restore CAS conflict")
        receipt = ExecutionTraceArtifactArchiveReceipt.model_validate(row.get("archive_receipt"))
        if receipt.offload_profile_id != profile.offload_profile_id:
            raise BrainValidationError("trace artifact restore profile mismatch")
        if revision == params.expected_revision + 1:
            envelope = await self.trace_artifact_archive.restore(receipt)
            staged = await self.storage.stage_restore_execution_trace_artifact(
                envelope=envelope,
                expected_revision=revision,
            )
            revision = as_int(staged.get("revision"))
        else:
            ProtectedExecutionTraceArtifactEnvelope.model_validate(row.get("protected_envelope"))
        await self.trace_artifact_archive.purge(receipt)
        restored = await self.storage.complete_restore_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
            expected_revision=revision,
        )
        return make_response(payload=restored, parent_unit=unit)

    @grpc_error_handler
    async def PurgeExecutionTraceArtifact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Apply one C0 lifecycle-profile CAS purge and preserve a tombstone."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        _validate_artifact_lifecycle_permission(token)
        params = PurgeExecutionTraceArtifactPayload.model_validate(unit.payload or {})
        validate_tenant_access(
            token,
            params.tenant_id,
            context,
            operation="write",
            record_kind="execution_trace_artifact",
        )
        _validate_project_binding(token, params.project_id)
        _protector, profile = self._artifact_protector_and_profile(params.lifecycle_profile_id)
        row = await self.storage.get_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
        )
        if row is None or row.get("lifecycle_profile_id") != params.lifecycle_profile_id:
            raise BrainValidationError("trace artifact purge binding mismatch")
        state = str(row.get("storage_state", ""))
        revision = as_int(row.get("revision"))
        if state == "purged" and revision == params.expected_revision + 2:
            return make_response(
                payload={
                    "artifact_id": str(params.artifact_id),
                    "storage_state": "purged",
                    "revision": revision,
                },
                parent_unit=unit,
            )
        if state in {"hot", "cold"} and revision == params.expected_revision:
            begun = await self.storage.begin_purge_execution_trace_artifact(
                tenant_id=params.tenant_id,
                project_id=params.project_id,
                artifact_id=str(params.artifact_id),
                expected_revision=params.expected_revision,
                legal_hold=profile.legal_hold,
            )
            revision = as_int(begun.get("revision"))
        elif state != "purging" or revision != params.expected_revision + 1:
            raise BrainValidationError("trace artifact purge lifecycle conflict")
        archive_receipt_raw = row.get("archive_receipt")
        if archive_receipt_raw is not None:
            if self.trace_artifact_archive is None:
                raise BrainValidationError("trace artifact archive profile is unavailable")
            archive_receipt = ExecutionTraceArtifactArchiveReceipt.model_validate(
                archive_receipt_raw
            )
            await self.trace_artifact_archive.purge(archive_receipt)
        receipt = await self.storage.purge_execution_trace_artifact(
            tenant_id=params.tenant_id,
            project_id=params.project_id,
            artifact_id=str(params.artifact_id),
            expected_revision=revision,
            legal_hold=profile.legal_hold,
        )
        return make_response(payload=receipt, parent_unit=unit)

    @grpc_stream_error_handler
    async def GetTraces(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Get agent traces with optional filters."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.TRACE_READ)
        params = GetTracesPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        rows = await self.storage.get_traces(
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            agent_id=params.agent_id,
            session_id=params.session_id,
            limit=params.limit,
            since=params.since,
        )

        for row in rows:
            yield make_response(
                payload={
                    "id": str(row.get("id", "")),
                    "agent_id": row.get("agent_id", ""),
                    "session_id": row.get("session_id", ""),
                    "user_id": row.get("user_id", ""),
                    "graph_name": row.get("graph_name", ""),
                    "tool_calls": row.get("tool_calls", []),
                    "token_usage": row.get("token_usage", {}),
                    "timing_ms": row.get("timing_ms"),
                    "security_flags": row.get("security_flags", {}),
                    "metadata": row.get("metadata", {}),
                    "provenance": row.get("provenance", []),
                    "graph_run_id": row.get("graph_run_id", ""),
                    "payload_digest": row.get("payload_digest", ""),
                    "terminal_status": row.get("terminal_status", ""),
                    "terminal_reason": row.get("terminal_reason", ""),
                    "trace_schema_version": row.get("trace_schema_version", "legacy_v0"),
                    "prompt_evidence": row.get("prompt_evidence", []),
                    "steps": row.get("steps", []),
                    "control_evidence": row.get("control_evidence", {}),
                    "created_at": str(row.get("created_at", "")),
                },
                parent_unit=unit,
            )


__all__ = ["TraceHandlersMixin"]
