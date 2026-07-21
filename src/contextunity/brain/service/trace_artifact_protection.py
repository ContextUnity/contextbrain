"""Brain-side application port for Shield-owned Trace artifact protection."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from contextunity.core.permissions import Permissions
from contextunity.core.sdk.clients import ShieldClient
from contextunity.core.sdk.execution_trace_artifacts import (
    ProtectedExecutionTraceArtifactEnvelope,
    ProtectExecutionTraceArtifactRequest,
    UnprotectedExecutionTraceArtifact,
    UnprotectExecutionTraceArtifactRequest,
)
from contextunity.core.tokens import ContextToken, ProjectBound, mint_service_token


@runtime_checkable
class SensitivePayloadProtector(Protocol):
    """Protect/unprotect without exposing long-lived key material to Brain."""

    async def protect(
        self,
        request: ProtectExecutionTraceArtifactRequest,
    ) -> ProtectedExecutionTraceArtifactEnvelope: ...

    async def unprotect(
        self,
        request: UnprotectExecutionTraceArtifactRequest,
    ) -> UnprotectedExecutionTraceArtifact: ...


class ShieldSensitivePayloadProtector:
    """Production adapter over Shield with a purpose-bound tenant token per call."""

    def __init__(self, *, host: str | None) -> None:
        self._host = host

    @staticmethod
    def _token(tenant_id: str, project_id: str) -> ContextToken:
        return mint_service_token(
            "brain-shield-trace-artifact-protection",
            permissions=(Permissions.SHIELD_TRACE_ARTIFACT_PROTECT,),
            allowed_tenants=(tenant_id,),
            project_binding=ProjectBound(project_id),
        )

    async def protect(
        self,
        request: ProtectExecutionTraceArtifactRequest,
    ) -> ProtectedExecutionTraceArtifactEnvelope:
        async with ShieldClient(
            host=self._host,
            token=self._token(
                request.identity.tenant_id,
                request.identity.project_id,
            ),
        ) as client:
            return await client.protect_execution_trace_artifact(request)

    async def unprotect(
        self,
        request: UnprotectExecutionTraceArtifactRequest,
    ) -> UnprotectedExecutionTraceArtifact:
        async with ShieldClient(
            host=self._host,
            token=self._token(
                request.envelope.identity.tenant_id,
                request.envelope.identity.project_id,
            ),
        ) as client:
            return await client.unprotect_execution_trace_artifact(request)


__all__ = ["SensitivePayloadProtector", "ShieldSensitivePayloadProtector"]
