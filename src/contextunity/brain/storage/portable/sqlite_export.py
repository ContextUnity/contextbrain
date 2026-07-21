"""Shared SQLite portable export/import typing guards."""

from __future__ import annotations

import sqlite3
from typing import Protocol, TypeGuard, runtime_checkable

from contextunity.brain.storage.contracts import BrainStorageProtocol


@runtime_checkable
class SqlitePortableExport(BrainStorageProtocol, Protocol):
    """SQLite store surface used for direct table export/import."""

    def get_sqlite_connection(self) -> sqlite3.Connection: ...

    def has_sqlite_vec(self) -> bool: ...


def is_sqlite_export_store(store: object) -> TypeGuard[SqlitePortableExport]:
    return hasattr(store, "get_sqlite_connection") and hasattr(store, "has_sqlite_vec")


__all__ = ["SqlitePortableExport", "is_sqlite_export_store"]
