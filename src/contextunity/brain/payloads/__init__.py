"""Pydantic payload models for Brain gRPC operations.

These models provide server-side validation for ContextUnit payloads.
Each RPC method extracts and validates its payload using these models.

Example usage in service:
    from .payloads import SearchCellsPayload

    async def SearchCells(self, request, context):
        unit = ContextUnit.from_protobuf(request)
        params = SearchCellsPayload.model_validate(unit.payload or {})
        # params.tenant_id, params.query_text, etc.
"""

from .admin import (
    AdminGetAnalyticsSummaryPayload,
    AdminGetCellsPayload,
    AdminGetFilterOptionsPayload,
    AdminGetMemoryLayerStatsPayload,
    AdminGetSessionTracesPayload,
    AdminGetSystemAnalyticsPayload,
    AdminGetTraceDetailsPayload,
    AdminSearchTracesPayload,
    ListTenantsPayload,
)
from .embedding import (
    ClaimCellEmbeddingJobsPayload,
    EmbedClaimedCellPayload,
    EnqueueCellEmbeddingPayload,
    FailCellEmbeddingJobPayload,
    GetCellEmbeddingStatusPayload,
    GetEmbeddingCapabilityPayload,
)
from .knowledge import (
    CreateKGRelationPayload,
    DeleteDocumentationCellsPayload,
    DocumentationDeleteTarget,
    GetCellPayload,
    GraphSearchPayload,
    IngestDocumentPayload,
    QueryCellsPayload,
    SearchCellsPayload,
    UpsertCellPayload,
)
from .memory import (
    AppendConversationRecordPayload,
    ApplyConversationRetentionPayload,
    ApplyExecutionTraceRetentionPayload,
    ArchiveExecutionTraceArtifactPayload,
    FinalizeExecutionTraceArtifactPayload,
    GetConversationHistoryStatsPayload,
    GetExecutionTraceArtifactPayload,
    GetTracesPayload,
    LogTracePayload,
    MatchDuckDBPayload,
    PruneExpiredBlackboardPayload,
    PurgeExecutionTraceArtifactPayload,
    QueryConversationHistoryPayload,
    ReadBlackboardPayload,
    ReserveExecutionTraceArtifactPayload,
    RestoreExecutionTraceArtifactPayload,
    WriteBlackboardPayload,
)
from .outcomes import OutcomeObservationPayload, ReportOutcomeObservationPayload
from .synapses import QuerySynapsesPayload, RecordSynapsePayload, UpdateSynapseQPayload
from .udb import (
    GetDebugCasePayload,
    QueryDebugCasesPayload,
    ReopenDebugCasePayload,
    ReportFaultOccurrencePayload,
    ReportMitigationAttemptPayload,
    ReportRecoveryEvidencePayload,
    ResolveDebugCasePayload,
)

__all__ = [
    "AppendConversationRecordPayload",
    "ApplyConversationRetentionPayload",
    "ApplyExecutionTraceRetentionPayload",
    "ArchiveExecutionTraceArtifactPayload",
    "AdminGetAnalyticsSummaryPayload",
    "AdminGetCellsPayload",
    "AdminGetFilterOptionsPayload",
    "AdminGetMemoryLayerStatsPayload",
    "AdminGetSessionTracesPayload",
    "AdminGetSystemAnalyticsPayload",
    "AdminGetTraceDetailsPayload",
    "AdminSearchTracesPayload",
    "ClaimCellEmbeddingJobsPayload",
    "CreateKGRelationPayload",
    "DeleteDocumentationCellsPayload",
    "DocumentationDeleteTarget",
    "EmbedClaimedCellPayload",
    "EnqueueCellEmbeddingPayload",
    "FailCellEmbeddingJobPayload",
    "FinalizeExecutionTraceArtifactPayload",
    "GetCellEmbeddingStatusPayload",
    "GetCellPayload",
    "GetConversationHistoryStatsPayload",
    "GetDebugCasePayload",
    "GetExecutionTraceArtifactPayload",
    "GetEmbeddingCapabilityPayload",
    "GetTracesPayload",
    "GraphSearchPayload",
    "IngestDocumentPayload",
    "ListTenantsPayload",
    "LogTracePayload",
    "MatchDuckDBPayload",
    "OutcomeObservationPayload",
    "PruneExpiredBlackboardPayload",
    "PurgeExecutionTraceArtifactPayload",
    "QueryCellsPayload",
    "QueryConversationHistoryPayload",
    "QueryDebugCasesPayload",
    "QuerySynapsesPayload",
    "ReadBlackboardPayload",
    "RecordSynapsePayload",
    "ReserveExecutionTraceArtifactPayload",
    "RestoreExecutionTraceArtifactPayload",
    "ReopenDebugCasePayload",
    "ReportFaultOccurrencePayload",
    "ReportMitigationAttemptPayload",
    "ReportOutcomeObservationPayload",
    "ReportRecoveryEvidencePayload",
    "ResolveDebugCasePayload",
    "SearchCellsPayload",
    "UpdateSynapseQPayload",
    "UpsertCellPayload",
    "WriteBlackboardPayload",
]
