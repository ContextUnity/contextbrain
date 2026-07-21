"""Payloads for Brain administrative RPCs."""

from contextunity.core.sdk.responses import MemoryLayerName
from contextunity.core.sdk.types import StrictPayloadModel
from contextunity.core.trace_inspection import TraceTerminalStatus
from pydantic import Field, model_validator

# Admin RPCs (WS-8) — require admin:read
# Cross-tenant observability owned by Brain (replaces View brain_db RLS bypass).


class ListTenantsPayload(StrictPayloadModel):
    """Payload for ListTenants admin RPC.

    No fields required — token scoping determines which tenants are returned.
    """


class AdminSearchTracesPayload(StrictPayloadModel):
    """Payload for AdminSearchTraces admin RPC.

    tenant_id is required unless the token has admin:all.
    The service enforces this: empty allowed_tenants is NEVER treated as "all tenants".
    """

    tenant_id: str | None = None
    service: str | None = None
    agent_id: str | None = None
    status: TraceTerminalStatus | None = None
    hours: int | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _reject_unbacked_service_filter(self) -> "AdminSearchTracesPayload":
        if self.service is not None:
            raise ValueError("service filter is unsupported by canonical Trace storage")
        return self


class AdminGetTraceDetailsPayload(StrictPayloadModel):
    """Payload for AdminGetTraceDetails admin RPC."""

    trace_id: str


class AdminGetSystemAnalyticsPayload(StrictPayloadModel):
    """Payload for AdminGetSystemAnalytics admin RPC.

    tenant_id is required unless the token has admin:all.
    """

    hours: int | None = None
    tenant_id: str | None = None


class AdminGetMemoryLayerStatsPayload(StrictPayloadModel):
    """Payload for AdminGetMemoryLayerStats admin RPC.

    tenant_id is required unless the token has admin:all.
    """

    layer: MemoryLayerName | None = None
    tenant_id: str | None = None


class AdminGetFilterOptionsPayload(StrictPayloadModel):
    """Payload for AdminGetFilterOptions admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None


class AdminGetSessionTracesPayload(StrictPayloadModel):
    """Payload for AdminGetSessionTraces admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    session_id: str
    tenant_id: str | None = None


class AdminGetCellsPayload(StrictPayloadModel):
    """Payload for AdminGetCells admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None
    kind: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class AdminGetAnalyticsSummaryPayload(StrictPayloadModel):
    """Payload for AdminGetAnalyticsSummary admin RPC.

    tenant_id is optional only when token has admin:all; otherwise required.
    """

    tenant_id: str | None = None
    hours: int | None = None
