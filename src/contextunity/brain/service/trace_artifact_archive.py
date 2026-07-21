"""Brain-side app port for Worker-owned encrypted artifact cold offload."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from contextunity.core.permissions import Permissions
from contextunity.core.sdk.clients import WorkerClient
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ProtectedExecutionTraceArtifactEnvelope,
)
from contextunity.core.tokens import ContextToken, PlatformBound, mint_service_token


@runtime_checkable
class TraceArtifactArchive(Protocol):
    """Cold-offload operations that never expose object locations to Brain callers."""

    async def archive(
        self,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        *,
        offload_profile_id: str,
        source_revision: int,
    ) -> ExecutionTraceArtifactArchiveReceipt: ...

    async def restore(
        self,
        receipt: ExecutionTraceArtifactArchiveReceipt,
    ) -> ProtectedExecutionTraceArtifactEnvelope: ...

    async def purge(self, receipt: ExecutionTraceArtifactArchiveReceipt) -> None: ...


class WorkerTraceArtifactArchive:
    """Production adapter over Worker encrypted-object lifecycle RPCs."""

    def __init__(self, *, host: str | None) -> None:
        self._host = host

    def _token(self, tenant_id: str) -> ContextToken:
        return mint_service_token(
            "brain-worker-trace-artifact-archive",
            permissions=(Permissions.WORKER_TRACE_ARTIFACT_ARCHIVE,),
            allowed_tenants=(tenant_id,),
            project_binding=PlatformBound(),
        )

    async def archive(
        self,
        envelope: ProtectedExecutionTraceArtifactEnvelope,
        *,
        offload_profile_id: str,
        source_revision: int,
    ) -> ExecutionTraceArtifactArchiveReceipt:
        async with WorkerClient(
            host=self._host,
            token=self._token(envelope.identity.tenant_id),
        ) as client:
            return await client.archive_execution_trace_artifact(
                envelope,
                offload_profile_id=offload_profile_id,
                source_revision=source_revision,
            )

    async def restore(
        self,
        receipt: ExecutionTraceArtifactArchiveReceipt,
    ) -> ProtectedExecutionTraceArtifactEnvelope:
        async with WorkerClient(
            host=self._host,
            token=self._token(receipt.identity.tenant_id),
        ) as client:
            return await client.restore_execution_trace_artifact(receipt)

    async def purge(self, receipt: ExecutionTraceArtifactArchiveReceipt) -> None:
        async with WorkerClient(
            host=self._host,
            token=self._token(receipt.identity.tenant_id),
        ) as client:
            await client.purge_execution_trace_artifact_archive(receipt)


__all__ = ["TraceArtifactArchive", "WorkerTraceArtifactArchive"]
