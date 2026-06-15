"""Factory for backend-specific admin query implementations."""

from __future__ import annotations

from contextunity.brain.core.exceptions import BrainStorageError
from contextunity.brain.storage.contracts import AdminQueryProtocol, KnowledgeStoreProtocol
from contextunity.brain.storage.postgres import PostgresKnowledgeStore
from contextunity.brain.storage.postgres.store.admin import PostgresAdminOps
from contextunity.brain.storage.sqlite.admin_ops import AsyncSqliteAdminOps
from contextunity.brain.storage.sqlite.store import SqliteVecStorageBackend


def create_admin_ops(storage: KnowledgeStoreProtocol) -> AdminQueryProtocol:
    """Return the admin query backend matching ``storage``."""
    if isinstance(storage, SqliteVecStorageBackend):
        return AsyncSqliteAdminOps(storage)
    if isinstance(storage, PostgresKnowledgeStore):
        return PostgresAdminOps(storage)
    msg = f"Admin queries are not supported for storage type {type(storage).__name__}"
    raise BrainStorageError(msg)


__all__ = ["create_admin_ops"]
