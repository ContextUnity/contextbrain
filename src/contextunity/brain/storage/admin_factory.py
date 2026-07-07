"""Factory for backend-specific admin query implementations."""

from __future__ import annotations

from contextunity.brain.core.exceptions import BrainStorageError
from contextunity.brain.storage.contracts import AdminQueryProtocol
from contextunity.brain.storage.postgres import PostgresBrainStore
from contextunity.brain.storage.postgres.store.admin import PostgresAdminOps
from contextunity.brain.storage.sqlite.admin_ops import AsyncSqliteAdminOps
from contextunity.brain.storage.sqlite.store import SqliteBrainStore


def create_admin_ops(storage: object) -> AdminQueryProtocol:
    """Return the admin query backend matching ``storage``."""
    if isinstance(storage, SqliteBrainStore):
        return AsyncSqliteAdminOps(storage)
    if isinstance(storage, PostgresBrainStore):
        return PostgresAdminOps(storage)
    msg = f"Admin queries are not supported for storage type {type(storage).__name__}"
    raise BrainStorageError(msg)


__all__ = ["create_admin_ops"]
