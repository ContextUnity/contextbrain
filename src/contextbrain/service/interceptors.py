"""gRPC interceptor for domain-specific permission enforcement.

Maps each Brain RPC method to the exact permission required
(brain:read, memory:write, trace:read, etc.) and validates
the ContextToken carries that permission + tenant access.

Delegates to ``contextcore.security.ServicePermissionInterceptor``
for unified enforcement logic. Brain only owns the RPC_PERMISSION_MAP.
"""

from __future__ import annotations

from contextcore.permissions import Permissions
from contextcore.security import (
    EnforcementMode,
    ServicePermissionInterceptor,
)

# ── RPC → Permission mapping ──────────────────────────────────

RPC_PERMISSION_MAP: dict[str, str] = {
    # Knowledge
    "Search": Permissions.BRAIN_READ,
    "IngestDocument": Permissions.BRAIN_WRITE,
    "GraphSearch": Permissions.BRAIN_READ,
    "GetTaxonomy": Permissions.BRAIN_READ,
    "DeleteDocument": Permissions.BRAIN_WRITE,
    # Memory
    "AddEpisode": Permissions.MEMORY_WRITE,
    "GetRecentEpisodes": Permissions.MEMORY_READ,
    "UpsertFact": Permissions.MEMORY_WRITE,
    "GetUserFacts": Permissions.MEMORY_READ,
    "RetentionCleanup": Permissions.MEMORY_WRITE,
    "GetEpisodeStats": Permissions.MEMORY_READ,
    # Traces
    "LogTrace": Permissions.TRACE_WRITE,
    "GetTraces": Permissions.TRACE_READ,
    # Commerce (via CommerceService — same server)
    "GetProduct": Permissions.BRAIN_READ,
    "UpsertProduct": Permissions.BRAIN_WRITE,
    "SearchProducts": Permissions.BRAIN_READ,
    "UpsertNewsPost": Permissions.BRAIN_WRITE,
    "GetNewsFeed": Permissions.BRAIN_READ,
    "CheckNewsPostExists": Permissions.BRAIN_READ,
}


class BrainPermissionInterceptor(ServicePermissionInterceptor):
    """Brain-specific permission interceptor.

    Thin wrapper around ``ServicePermissionInterceptor`` that pre-fills
    the Brain RPC permission map and service name.

    Usage::

        interceptor = BrainPermissionInterceptor(enforcement=EnforcementMode.WARN)
        server = grpc.aio.server(interceptors=[interceptor])
    """

    def __init__(self, *, enforcement: EnforcementMode | None = None) -> None:
        super().__init__(
            RPC_PERMISSION_MAP,
            service_name="Brain",
            enforcement=enforcement,
        )


__all__ = [
    "BrainPermissionInterceptor",
    "RPC_PERMISSION_MAP",
]
