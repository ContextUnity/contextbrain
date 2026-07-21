from __future__ import annotations

from base64 import b64encode
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import grpc
import pytest
from contextunity.core import ContextToken, ContextUnit, contextunit_pb2
from contextunity.core.permissions import Permissions
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ExecutionTraceArtifactIdentity,
    ExecutionTraceArtifactLifecycleProfile,
    ModelIOContentPart,
    ProtectedExecutionTraceArtifactEnvelope,
    ProtectedModelIOSettings,
    ProtectExecutionTraceArtifactRequest,
    UnprotectExecutionTraceArtifactRequest,
)
from contextunity.core.tokens import ProjectBound
from contextunity.core.types import GrpcServicerContext
from contextunity.shield.trace_artifact_protector import TraceArtifactProtector
from cryptography.fernet import Fernet

from contextunity.brain.service.handlers.traces import TraceHandlersMixin
from contextunity.brain.storage.sqlite import SqliteBrainStore

ARTIFACT_ID = UUID("11111111-1111-4111-8111-111111111111")


class _GrpcContext:
    def __init__(self) -> None:
        self.code: object | None = None
        self.details: str | None = None

    async def abort(self, code: object, details: str) -> None:
        self.code = code
        self.details = details
        raise grpc.RpcError("abort")

    def set_code(self, code: object) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, metadata: object) -> None:
        _ = metadata

    def invocation_metadata(self) -> tuple[tuple[str, str | bytes], ...]:
        return ()


assert isinstance(_GrpcContext(), GrpcServicerContext)


class AsyncProtector:
    def __init__(self) -> None:
        self._delegate = TraceArtifactProtector(Fernet.generate_key().decode())

    async def protect(self, request: ProtectExecutionTraceArtifactRequest):
        return self._delegate.protect(request)

    async def unprotect(self, request: UnprotectExecutionTraceArtifactRequest):
        return self._delegate.unprotect(request)


class AsyncArchive:
    def __init__(self) -> None:
        self.envelope: ProtectedExecutionTraceArtifactEnvelope | None = None
        self.archive_calls = 0
        self.purged = False

    async def archive(
        self,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        *,
        offload_profile_id: str,
        source_revision: int,
    ) -> ExecutionTraceArtifactArchiveReceipt:
        self.archive_calls += 1
        self.envelope = envelope
        return ExecutionTraceArtifactArchiveReceipt(
            artifact_id=envelope.artifact_id,
            identity=envelope.identity,
            content_digest=envelope.content_digest,
            offload_profile_id=offload_profile_id,
            archive_generation=uuid4(),
            source_revision=source_revision,
        )

    async def restore(
        self,
        receipt: ExecutionTraceArtifactArchiveReceipt,
    ) -> ProtectedExecutionTraceArtifactEnvelope:
        assert self.envelope is not None
        assert self.envelope.artifact_id == receipt.artifact_id
        return self.envelope

    async def purge(self, receipt: ExecutionTraceArtifactArchiveReceipt) -> None:
        assert receipt.artifact_id == ARTIFACT_ID
        self.purged = True


class TraceServiceForTest(TraceHandlersMixin):
    def __init__(self, storage: SqliteBrainStore) -> None:
        self.storage = storage
        self.trace_artifact_protector = AsyncProtector()
        self.trace_artifact_archive = AsyncArchive()
        self.trace_artifact_settings = ProtectedModelIOSettings(
            protector="shield_rpc",
            lifecycle_profiles=(
                ExecutionTraceArtifactLifecycleProfile(
                    profile_id="trace-artifacts-standard",
                    hot_for_days=7,
                    archive_after_days=7,
                    purge_after_days=365,
                    offload_profile_id="cold-a",
                ),
            ),
        )


def _identity() -> ExecutionTraceArtifactIdentity:
    return ExecutionTraceArtifactIdentity(
        tenant_id="tenant-a",
        project_id="project-a",
        trace_id=UUID("22222222-2222-4222-8222-222222222222"),
        graph_run_id=UUID("33333333-3333-4333-8333-333333333333"),
        invocation_id=UUID("44444444-4444-4444-8444-444444444444"),
        provider_attempt_id=UUID("55555555-5555-4555-8555-555555555555"),
        artifact_kind="model_io",
    )


def _part(sequence: int, channel: str, content: str) -> dict[str, object]:
    return ModelIOContentPart(
        sequence=sequence,
        channel=channel,
        content_kind="text",
        mime_type="text/plain",
        content=content,
        byte_count=len(content.encode()),
    ).model_dump(mode="json")


def _token(project_id: str = "project-a") -> ContextToken:
    return ContextToken(
        token_id="trace-artifact-test",
        project_binding=ProjectBound(project_id),
        permissions=(
            Permissions.TRACE_READ,
            Permissions.TRACE_ARTIFACT_READ,
            Permissions.TRACE_ARTIFACT_LIFECYCLE,
            Permissions.TRACE_WRITE,
        ),
        allowed_tenants=("tenant-a",),
    )


@pytest.mark.asyncio
async def test_artifact_handler_round_trip_and_tombstone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import traces as handler_module

    service = TraceServiceForTest(
        SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    )
    monkeypatch.setattr(handler_module, "extract_token_from_context", lambda _ctx: _token())
    reserve = ContextUnit(
        payload={
            "identity": _identity().model_dump(mode="json"),
            "artifact_id": str(ARTIFACT_ID),
            "lifecycle_profile_id": "trace-artifacts-standard",
            "capture_policy_version": "contextunity.model-io-capture/v1",
            "request_parts": [_part(0, "user", "hello")],
        }
    )
    reserved = ContextUnit.from_protobuf(
        await service.ReserveExecutionTraceArtifact(
            reserve.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert reserved["revision"] == 1
    read = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": str(ARTIFACT_ID),
        }
    )
    reserved_detail = ContextUnit.from_protobuf(
        await service.GetExecutionTraceArtifact(
            read.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert reserved_detail["reservation"]["request_parts"][0]["content"] == "hello"
    assert "content" not in reserved_detail
    assert "reservation_digest" not in reserved_detail

    finalize = ContextUnit(
        payload={
            "identity": _identity().model_dump(mode="json"),
            "artifact_id": str(ARTIFACT_ID),
            "expected_revision": 1,
            "provider_status": "succeeded",
            "response_parts": [_part(0, "assistant", "world")],
        }
    )
    finalized = ContextUnit.from_protobuf(
        await service.FinalizeExecutionTraceArtifact(
            finalize.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert finalized["artifact_ref"]["storage_state"] == "hot"
    replayed = ContextUnit.from_protobuf(
        await service.FinalizeExecutionTraceArtifact(
            finalize.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert replayed["outcome"] == "duplicate"
    assert replayed["artifact_ref"]["storage_state"] == "hot"

    archive = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": str(ARTIFACT_ID),
            "expected_revision": 2,
            "lifecycle_profile_id": "trace-artifacts-standard",
        }
    )
    archived = ContextUnit.from_protobuf(
        await service.ArchiveExecutionTraceArtifact(
            archive.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert archived["storage_state"] == "cold"
    replayed_archive = ContextUnit.from_protobuf(
        await service.ArchiveExecutionTraceArtifact(
            archive.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert replayed_archive["storage_state"] == "cold"
    assert service.trace_artifact_archive.archive_calls == 1

    cold_detail = ContextUnit.from_protobuf(
        await service.GetExecutionTraceArtifact(
            read.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert cold_detail["storage_state"] == "cold"
    assert "content" not in cold_detail
    assert "archive_receipt" not in cold_detail

    restore = ContextUnit(
        payload={
            **read.payload,
            "expected_revision": 4,
            "lifecycle_profile_id": "trace-artifacts-standard",
        }
    )
    restored = ContextUnit.from_protobuf(
        await service.RestoreExecutionTraceArtifact(
            restore.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert restored["storage_state"] == "hot"
    detail = ContextUnit.from_protobuf(
        await service.GetExecutionTraceArtifact(
            read.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert detail["content"]["request_parts"][0]["content"] == "hello"
    assert detail["content"]["response_parts"][0]["content"] == "world"

    purge = ContextUnit(
        payload={
            **read.payload,
            "expected_revision": 7,
            "lifecycle_profile_id": "trace-artifacts-standard",
        }
    )
    await service.PurgeExecutionTraceArtifact(purge.to_protobuf(contextunit_pb2), SimpleNamespace())
    tombstone = ContextUnit.from_protobuf(
        await service.GetExecutionTraceArtifact(
            read.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ).payload
    assert tombstone["storage_state"] == "purged"
    assert "content" not in tombstone


@pytest.mark.asyncio
async def test_protected_artifact_read_requires_namespace_permission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import traces as handler_module

    service = TraceServiceForTest(
        SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    )
    coarse_only = ContextToken(
        token_id="trace-coarse-only",
        project_binding=ProjectBound("project-a"),
        permissions=(Permissions.TRACE_READ,),
        allowed_tenants=("tenant-a",),
    )
    monkeypatch.setattr(
        handler_module,
        "extract_token_from_context",
        lambda _ctx: coarse_only,
    )
    unit = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": str(ARTIFACT_ID),
        }
    )

    context = _GrpcContext()
    with pytest.raises(grpc.RpcError):
        await service.GetExecutionTraceArtifact(unit.to_protobuf(contextunit_pb2), context)
    assert context.code == grpc.StatusCode.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_reserve_fails_before_storage_when_project_binding_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import traces as handler_module

    service = TraceServiceForTest(
        SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)
    )
    monkeypatch.setattr(
        handler_module,
        "extract_token_from_context",
        lambda _ctx: _token("project-b"),
    )
    unit = ContextUnit(
        payload={
            "identity": _identity().model_dump(mode="json"),
            "artifact_id": str(ARTIFACT_ID),
            "lifecycle_profile_id": "trace-artifacts-standard",
            "capture_policy_version": "contextunity.model-io-capture/v1",
            "request_parts": [_part(0, "user", b64encode(b"secret").decode())],
        }
    )

    context = _GrpcContext()
    with pytest.raises(grpc.RpcError):
        await service.ReserveExecutionTraceArtifact(unit.to_protobuf(contextunit_pb2), context)
    assert context.code == grpc.StatusCode.PERMISSION_DENIED
    assert (
        await service.storage.get_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(ARTIFACT_ID),
        )
        is None
    )
