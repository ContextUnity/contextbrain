"""BrainService - unified gRPC service using ContextUnit protocol.
Composed of modular handler mixins for different domains.
"""

from __future__ import annotations

from contextunity.core import brain_pb2_grpc, get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError

from ..storage.contracts import BrainStorageProtocol
from ..storage.duckdb_store import DuckDBStore
from ..storage.postgres import PostgresBrainStore
from .embeddings import Embedder, get_embedder
from .handler_base import BrainHandlerBase
from .handlers import (
    AdminHandlersMixin,
    BlackboardHandlersMixin,
    CommerceHandlersMixin,
    EmbeddingHandlersMixin,
    KnowledgeHandlersMixin,
    MemoryHandlersMixin,
    SynapseHandlersMixin,
    TaxonomyHandlersMixin,
    TraceHandlersMixin,
)

logger = get_contextunit_logger(__name__)


class BrainService(
    KnowledgeHandlersMixin,
    EmbeddingHandlersMixin,
    MemoryHandlersMixin,
    TraceHandlersMixin,
    TaxonomyHandlersMixin,
    CommerceHandlersMixin,
    BlackboardHandlersMixin,
    SynapseHandlersMixin,
    AdminHandlersMixin,
    BrainHandlerBase,
    brain_pb2_grpc.BrainServiceServicer,
):
    """Unified implementation of the Brain gRPC service using ContextUnit.

    Composed of modular handler mixins:
    - KnowledgeHandlersMixin: search, upsert, KG operations
    - EmbeddingHandlersMixin: durable cell embedding jobs and status
    - MemoryHandlersMixin: episodes, facts
    - TraceHandlersMixin: agent execution traces
    - TaxonomyHandlersMixin: taxonomy CRUD
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
    ) -> None:
        """Initialize BrainService with optional injected backends (tests/local mode)."""
        if storage is not None:
            super().__init__(
                storage=storage,
                duckdb=duckdb or DuckDBStore(),
                embedder=embedder or get_embedder(),
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
        super().__init__(
            storage=PostgresBrainStore(
                dsn=dsn,
                schema=config.schema_name,
            ),
            duckdb=DuckDBStore(),
            embedder=get_embedder(config),
        )


__all__ = ["BrainService"]
