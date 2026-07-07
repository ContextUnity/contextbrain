"""gRPC interceptor for domain-specific permission enforcement.
Maps each Brain RPC method to the exact permission required
(brain:read, memory:write, trace:read, etc.) and validates
the ContextToken carries that permission + tenant access.
Delegates to ``contextunity.core.security.ServicePermissionInterceptor``
for unified enforcement logic. Brain only owns the RPC_PERMISSION_MAP.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from contextunity.core.permissions import Permissions
from contextunity.core.security import (
    ServicePermissionInterceptor,
)

if TYPE_CHECKING:
    from contextunity.brain.core.config.main import BrainConfig

# ── RPC → Permission mapping ──────────────────────────────────

RPC_PERMISSION_MAP: dict[str, str] = {
    # Knowledge
    "Search": Permissions.BRAIN_READ,
    "GraphSearch": Permissions.BRAIN_READ,
    "CreateKGRelation": Permissions.BRAIN_WRITE,
    "Upsert": Permissions.BRAIN_WRITE,
    "QueryMemory": Permissions.BRAIN_READ,
    "GetTaxonomy": Permissions.BRAIN_READ,
    # Memory
    "AddEpisode": Permissions.MEMORY_WRITE,
    "GetRecentEpisodes": Permissions.MEMORY_READ,
    "UpsertFact": Permissions.MEMORY_WRITE,
    "GetUserFacts": Permissions.MEMORY_READ,
    "RetentionCleanup": Permissions.MEMORY_WRITE,
    "GetEpisodeStats": Permissions.MEMORY_READ,
    "WriteBlackboard": Permissions.MEMORY_WRITE,
    "ReadBlackboard": Permissions.MEMORY_READ,
    # BrainSynapse — memory:* family, same as Blackboard/episodes above.
    # UpdateSynapseQ may later warrant a dedicated review permission for
    # admin-verdict updates, but memory:write is correct for the general
    # case today.
    "RecordSynapse": Permissions.MEMORY_WRITE,
    "QuerySynapses": Permissions.MEMORY_READ,
    "UpdateSynapseQ": Permissions.MEMORY_WRITE,
    # Traces
    "LogTrace": Permissions.TRACE_WRITE,
    "GetTraces": Permissions.TRACE_READ,
    # Taxonomy
    "UpsertTaxonomy": Permissions.BRAIN_WRITE,
    # Other
    "MatchDuckDB": Permissions.BRAIN_READ,
    # Admin RPCs (WS-8) — cross-tenant observability; all require admin:read.
    # Tenant scoping is enforced additionally inside the handler via
    # _require_admin_tenant_scope(): empty allowed_tenants is NEVER "all tenants".
    "ListTenants": Permissions.ADMIN_READ,
    "AdminSearchTraces": Permissions.ADMIN_READ,
    "AdminGetTraceDetails": Permissions.ADMIN_READ,
    "AdminGetSystemAnalytics": Permissions.ADMIN_READ,
    "AdminGetMemoryLayerStats": Permissions.ADMIN_READ,
    "AdminGetFilterOptions": Permissions.ADMIN_READ,
    "AdminGetSessionTraces": Permissions.ADMIN_READ,
    "AdminGetRelatedEpisodes": Permissions.ADMIN_READ,
    "AdminSearchEpisodes": Permissions.ADMIN_READ,
    "AdminGetCells": Permissions.ADMIN_READ,
    "AdminGetAnalyticsSummary": Permissions.ADMIN_READ,
}


class BrainPermissionInterceptor(ServicePermissionInterceptor):
    """Brain-specific permission interceptor.

    Thin wrapper around ``ServicePermissionInterceptor`` that pre-fills
    the Brain RPC permission map and service name.

    Usage::

        interceptor = BrainPermissionInterceptor()
        server = grpc.aio.server(interceptors=[interceptor])
    """

    def __init__(self, *, shield_url: str = "", config: "BrainConfig | None" = None) -> None:
        """Initialize a new instance of BrainPermissionInterceptor."""
        super().__init__(
            RPC_PERMISSION_MAP,
            service_name="Brain",
            shield_url=shield_url,
            config=config,
        )


__all__ = [
    "BrainPermissionInterceptor",
    "RPC_PERMISSION_MAP",
]
