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
    "SearchCells": Permissions.BRAIN_READ,
    "GraphSearch": Permissions.BRAIN_READ,
    "CreateKGRelation": Permissions.BRAIN_WRITE,
    "IngestDocument": Permissions.BRAIN_WRITE,
    "UpsertCell": Permissions.BRAIN_WRITE,
    "QueryCells": Permissions.BRAIN_READ,
    "GetCell": Permissions.BRAIN_READ,
    "DeleteDocumentationCells": Permissions.BRAIN_WRITE,
    "EnqueueCellEmbedding": Permissions.BRAIN_WRITE,
    "ClaimCellEmbeddingJobs": Permissions.BRAIN_EMBED,
    "EmbedClaimedCell": Permissions.BRAIN_EMBED,
    "FailCellEmbeddingJob": Permissions.BRAIN_EMBED,
    "GetCellEmbeddingStatus": Permissions.BRAIN_READ,
    "GetEmbeddingCapability": Permissions.BRAIN_EMBED,
    # Memory
    "AppendConversationRecord": Permissions.MEMORY_WRITE,
    "QueryConversationHistory": Permissions.CONVERSATION_READ,
    "GetConversationHistoryStats": Permissions.CONVERSATION_READ,
    "ApplyConversationRetention": Permissions.MEMORY_WRITE,
    "ApplyExecutionTraceRetention": Permissions.TRACE_WRITE,
    "WriteBlackboard": Permissions.MEMORY_WRITE,
    "ReadBlackboard": Permissions.MEMORY_READ,
    "PruneExpiredBlackboard": Permissions.MEMORY_WRITE,
    # BrainSynapse — memory:* family, same as Blackboard/Conversation History.
    # UpdateSynapseQ may later warrant a dedicated review permission for
    # admin-verdict updates, but memory:write is correct for the general
    # case today.
    "RecordSynapse": Permissions.MEMORY_WRITE,
    "QuerySynapses": Permissions.MEMORY_READ,
    "UpdateSynapseQ": Permissions.MEMORY_WRITE,
    "ReportOutcomeObservation": Permissions.ADMIN_WRITE,
    # Traces
    "LogTrace": Permissions.TRACE_WRITE,
    "GetTraces": Permissions.TRACE_READ,
    "ReserveExecutionTraceArtifact": Permissions.TRACE_WRITE,
    "FinalizeExecutionTraceArtifact": Permissions.TRACE_WRITE,
    "GetExecutionTraceArtifact": Permissions.TRACE_ARTIFACT_READ,
    "ArchiveExecutionTraceArtifact": Permissions.TRACE_ARTIFACT_LIFECYCLE,
    "RestoreExecutionTraceArtifact": Permissions.TRACE_ARTIFACT_LIFECYCLE,
    "PurgeExecutionTraceArtifact": Permissions.TRACE_ARTIFACT_LIFECYCLE,
    # UniversalDebugBus uses the trace evidence capability family until a
    # dedicated debug-case capability is introduced with operator issuance.
    "ReportFaultOccurrence": Permissions.TRACE_WRITE,
    "ReportMitigationAttempt": Permissions.TRACE_WRITE,
    "ReportRecoveryEvidence": Permissions.TRACE_WRITE,
    "ResolveDebugCase": Permissions.TRACE_WRITE,
    "ReopenDebugCase": Permissions.TRACE_WRITE,
    "GetDebugCase": Permissions.TRACE_READ,
    "QueryDebugCases": Permissions.TRACE_READ,
    "QueryRecurringFaults": Permissions.TRACE_READ,
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
            allow_local_platform_hmac=True,
        )


__all__ = [
    "BrainPermissionInterceptor",
    "RPC_PERMISSION_MAP",
]
