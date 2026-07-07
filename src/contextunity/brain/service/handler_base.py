"""Typed attributes shared by Brain gRPC handler mixins."""

from __future__ import annotations

from contextunity.brain.storage.admin_factory import create_admin_ops
from contextunity.brain.storage.contracts import AdminQueryProtocol, BrainStorageProtocol
from contextunity.brain.storage.duckdb_store import DuckDBStore

from .embedders import ApiEmbedder, LocalEmbedder


class BrainHandlerBase:
    """Initialized via ``super().__init__`` from ``BrainService`` (last mixin before servicer)."""

    storage: BrainStorageProtocol
    _admin_ops: AdminQueryProtocol
    duckdb: DuckDBStore | None
    embedder: ApiEmbedder | LocalEmbedder

    def __init__(
        self,
        *,
        storage: BrainStorageProtocol,
        duckdb: DuckDBStore | None,
        embedder: ApiEmbedder | LocalEmbedder,
    ) -> None:
        self.storage = storage
        self._admin_ops = create_admin_ops(storage)
        self.duckdb = duckdb
        self.embedder = embedder
        super().__init__()


__all__ = ["BrainHandlerBase"]
