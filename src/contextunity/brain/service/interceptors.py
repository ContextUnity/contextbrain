"""gRPC interceptor for domain-specific permission enforcement.

Maps each Brain RPC method to the exact permission required
(brain:read, memory:write, trace:read, etc.) and validates
the ContextToken carries that permission + tenant access.

Delegates to ``contextunity.core.security.ServicePermissionInterceptor``
for unified enforcement logic. Brain only owns the RPC_PERMISSION_MAP.
"""

from __future__ import annotations

from contextunity.core.permissions import Permissions
from contextunity.core.security import (
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
    # Taxonomy
    "UpsertTaxonomy": Permissions.BRAIN_WRITE,
    "CreateKGRelation": Permissions.BRAIN_WRITE,
    # Other
    "MatchDuckDB": Permissions.BRAIN_READ,
}


class BrainPermissionInterceptor(ServicePermissionInterceptor):
    """Brain-specific permission interceptor.

    Thin wrapper around ``ServicePermissionInterceptor`` that pre-fills
    the Brain RPC permission map and service name.

    Usage::

        interceptor = BrainPermissionInterceptor()
        server = grpc.aio.server(interceptors=[interceptor])
    """

    def __init__(self, *, shield_url: str = "") -> None:
        super().__init__(
            RPC_PERMISSION_MAP,
            service_name="Brain",
            shield_url=shield_url,
        )


__all__ = [
    "BrainPermissionInterceptor",
    "RPC_PERMISSION_MAP",
]
