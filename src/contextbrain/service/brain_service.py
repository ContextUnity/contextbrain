"""BrainService - unified gRPC service using ContextUnit protocol.

Composed of modular handler mixins for different domains.
"""

from __future__ import annotations

import os

from contextcore import brain_pb2_grpc, get_context_unit_logger

from ..storage.duckdb_store import DuckDBStore
from ..storage.postgres import NewsStore, PostgresKnowledgeStore
from .embedders import get_embedder
from .handlers import (
    CommerceHandlersMixin,
    KnowledgeHandlersMixin,
    MemoryHandlersMixin,
    NewsHandlersMixin,
    TaxonomyHandlersMixin,
)

logger = get_context_unit_logger(__name__)


class BrainService(
    KnowledgeHandlersMixin,
    MemoryHandlersMixin,
    TaxonomyHandlersMixin,
    NewsHandlersMixin,
    CommerceHandlersMixin,
    brain_pb2_grpc.BrainServiceServicer,
):
    """Unified implementation of the Brain gRPC service using ContextUnit.

    Composed of modular handler mixins:
    - KnowledgeHandlersMixin: search, upsert, KG operations
    - MemoryHandlersMixin: episodes, facts
    - TaxonomyHandlersMixin: taxonomy CRUD
    - NewsHandlersMixin: news items and posts
    - CommerceHandlersMixin: verifications
    """

    def __init__(self):
        dsn = os.getenv("BRAIN_DATABASE_URL") or os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError(
                "BRAIN_DATABASE_URL or DATABASE_URL must be set. "
                "Example: postgresql://brain:brain_dev@localhost:5433/brain"
            )
        self.storage = PostgresKnowledgeStore(dsn=dsn)
        self.news_store = NewsStore(dsn=dsn)
        self.duckdb = DuckDBStore()
        self.embedder = get_embedder()


__all__ = ["BrainService"]
