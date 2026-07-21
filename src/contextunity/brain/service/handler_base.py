"""Typed attributes shared by Brain gRPC handler mixins."""

from __future__ import annotations

from contextunity.core.sdk.execution_trace_artifacts import ProtectedModelIOSettings

from contextunity.brain.storage.admin_factory import create_admin_ops
from contextunity.brain.storage.contracts import AdminQueryProtocol, BrainStorageProtocol
from contextunity.brain.storage.duckdb_store import DuckDBStore

from .embeddings import Embedder
from .trace_artifact_archive import TraceArtifactArchive
from .trace_artifact_protection import SensitivePayloadProtector


class BrainHandlerBase:
    """Initialized via ``super().__init__`` from ``BrainService`` (last mixin before servicer)."""

    storage: BrainStorageProtocol
    _admin_ops: AdminQueryProtocol
    duckdb: DuckDBStore | None
    embedder: Embedder
    trace_artifact_protector: SensitivePayloadProtector | None
    trace_artifact_archive: TraceArtifactArchive | None
    trace_artifact_settings: ProtectedModelIOSettings

    def __init__(
        self,
        *,
        storage: BrainStorageProtocol,
        duckdb: DuckDBStore | None,
        embedder: Embedder,
        trace_artifact_protector: SensitivePayloadProtector | None = None,
        trace_artifact_archive: TraceArtifactArchive | None = None,
        trace_artifact_settings: ProtectedModelIOSettings | None = None,
    ) -> None:
        self.storage = storage
        self._admin_ops = create_admin_ops(storage)
        self.duckdb = duckdb
        self.embedder = embedder
        self.trace_artifact_protector = trace_artifact_protector
        self.trace_artifact_archive = trace_artifact_archive
        self.trace_artifact_settings = trace_artifact_settings or ProtectedModelIOSettings()
        super().__init__()


__all__ = ["BrainHandlerBase"]
