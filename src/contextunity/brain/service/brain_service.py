"""BrainService - unified gRPC service using ContextUnit protocol.

Composed of modular handler mixins for different domains.
"""

from __future__ import annotations

from contextunity.core import brain_pb2_grpc, get_contextunit_logger

from ..storage.duckdb_store import DuckDBStore
from ..storage.postgres import PostgresKnowledgeStore
from .embedders import get_embedder
from .handlers import (
    CommerceHandlersMixin,
    KnowledgeHandlersMixin,
    MemoryHandlersMixin,
    TaxonomyHandlersMixin,
    TraceHandlersMixin,
)

logger = get_contextunit_logger(__name__)


class BrainService(
    KnowledgeHandlersMixin,
    MemoryHandlersMixin,
    TraceHandlersMixin,
    TaxonomyHandlersMixin,
    CommerceHandlersMixin,
    brain_pb2_grpc.BrainServiceServicer,
):
    """Unified implementation of the Brain gRPC service using ContextUnit.

    Composed of modular handler mixins:
    - KnowledgeHandlersMixin: search, upsert, KG operations
    - MemoryHandlersMixin: episodes, facts
    - TraceHandlersMixin: agent execution traces
    - TaxonomyHandlersMixin: taxonomy CRUD
    - CommerceHandlersMixin: verifications
    """

    def __init__(self):
        from contextunity.brain.core import get_core_config

        config = get_core_config()
        dsn = config.database_url
        if not dsn:
            raise RuntimeError(
                "BRAIN_DATABASE_URL or DATABASE_URL must be set. "
                "Example: postgresql://brain:brain_dev@localhost:5433/brain"
            )
        self.storage = PostgresKnowledgeStore(
            dsn=dsn,
            schema=config.schema_name,
        )
        self.duckdb = DuckDBStore()
        self.embedder = get_embedder(config)


__all__ = ["BrainService"]
