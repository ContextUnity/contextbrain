"""BrainService - unified gRPC service using ContextUnit protocol.
Composed of modular handler mixins for different domains.
"""

from __future__ import annotations

from contextunity.core import brain_pb2_grpc, get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.sdk.execution_trace_artifacts import ProtectedModelIOSettings

from ..storage.contracts import BrainStorageProtocol
from ..storage.duckdb_store import DuckDBStore
from ..storage.postgres import PostgresBrainStore
from .embeddings import Embedder, get_embedder
from .handler_base import BrainHandlerBase
from .handlers import (
    AdminHandlersMixin,
    BlackboardHandlersMixin,
    CellEdgeHandlersMixin,
    CellSearchHandlersMixin,
    CellWriteHandlersMixin,
    CommerceHandlersMixin,
    EmbeddingHandlersMixin,
    MemoryHandlersMixin,
    OutcomeObservationHandlersMixin,
    SynapseHandlersMixin,
    TraceHandlersMixin,
    UdbHandlersMixin,
)
from .trace_artifact_archive import TraceArtifactArchive, WorkerTraceArtifactArchive
from .trace_artifact_protection import (
    SensitivePayloadProtector,
    ShieldSensitivePayloadProtector,
)

logger = get_contextunit_logger(__name__)


class BrainService(
    CellSearchHandlersMixin,
    CellWriteHandlersMixin,
    CellEdgeHandlersMixin,
    EmbeddingHandlersMixin,
    MemoryHandlersMixin,
    TraceHandlersMixin,
    UdbHandlersMixin,
    CommerceHandlersMixin,
    BlackboardHandlersMixin,
    SynapseHandlersMixin,
    OutcomeObservationHandlersMixin,
    AdminHandlersMixin,
    BrainHandlerBase,
    brain_pb2_grpc.BrainServiceServicer,
):
    """Unified implementation of the Brain gRPC service using ContextUnit.

    Composed of modular handler mixins:
    - CellSearchHandlersMixin: canonical ranked BrainCell retrieval
    - CellWriteHandlersMixin: canonical cells and explicit document ingestion
    - CellEdgeHandlersMixin: retained graph operations pending Phase 5
    - EmbeddingHandlersMixin: durable cell embedding jobs and status
    - MemoryHandlersMixin: Conversation History and retention
    - TraceHandlersMixin: agent execution traces
    - CommerceHandlersMixin: verifications
    - BlackboardHandlersMixin: blackboard read/write (Flat Memory)
    - SynapseHandlersMixin: BrainSynapse record/query/update-Q (Flat Memory Phase B)
    - AdminHandlersMixin: cross-tenant admin observability (WS-8)
    """

    def __init__(
        self,
        storage: BrainStorageProtocol | None = None,
        duckdb: DuckDBStore | None = None,
        embedder: Embedder | None = None,
        trace_artifact_protector: SensitivePayloadProtector | None = None,
        trace_artifact_archive: TraceArtifactArchive | None = None,
        trace_artifact_settings: ProtectedModelIOSettings | None = None,
    ) -> None:
        """Initialize BrainService with optional injected backends (tests/local mode)."""
        if storage is not None:
            super().__init__(
                storage=storage,
                duckdb=duckdb or DuckDBStore(),
                embedder=embedder or get_embedder(),
                trace_artifact_protector=trace_artifact_protector,
                trace_artifact_archive=trace_artifact_archive,
                trace_artifact_settings=trace_artifact_settings,
            )
            return

        from contextunity.brain.core import get_core_config

        config = get_core_config()
        dsn = config.postgres.dsn
        if not dsn:
            raise ConfigurationError(
                (
                    "POSTGRES_DSN must be set. "
                    "Example: postgresql://brain:brain_dev@localhost:5433/brain"
                )
            )
        resolved_protector = trace_artifact_protector
        if resolved_protector is None and config.trace_artifacts.protector == "shield_rpc":
            resolved_protector = ShieldSensitivePayloadProtector(host=config.shield_url or None)
        resolved_archive = trace_artifact_archive
        if resolved_archive is None and any(
            profile.offload_profile_id is not None
            for profile in config.trace_artifacts.lifecycle_profiles
        ):
            resolved_archive = WorkerTraceArtifactArchive(host=config.worker_url or None)
        super().__init__(
            storage=PostgresBrainStore(
                dsn=dsn,
                schema=config.schema_name,
            ),
            duckdb=DuckDBStore(),
            embedder=get_embedder(config),
            trace_artifact_protector=resolved_protector,
            trace_artifact_archive=resolved_archive,
            trace_artifact_settings=config.trace_artifacts,
        )


__all__ = ["BrainService"]
