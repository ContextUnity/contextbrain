"""SQLite connection lifecycle management."""

from __future__ import annotations

import importlib
import sqlite3
import types
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncContextManager

from contextunity.core import get_contextunit_logger

logger = get_contextunit_logger(__name__)

_sqlite_vec_module: types.ModuleType | None
try:
    _sqlite_vec_module = importlib.import_module("sqlite_vec")
except ImportError:
    _sqlite_vec_module = None


class SqliteConnectionMixin:
    """Provides ``_get_connection()`` for domain mixins.

    Handles sqlite_vec extension loading, WAL mode, and row factory.
    """

    db_path: Path = Path()
    vector_dim: int = 0

    def _get_connection(self) -> sqlite3.Connection:
        """get connection.

        Returns:
            sqlite3.Connection: An instance of sqlite3.Connection.
        """
        db = sqlite3.connect(str(self.db_path), check_same_thread=False)
        _ = db.execute("PRAGMA journal_mode=WAL;")
        _ = db.execute("PRAGMA foreign_keys=ON;")
        db.enable_load_extension(True)
        if _sqlite_vec_module is not None:
            load_ext = getattr(_sqlite_vec_module, "load", None)
            if callable(load_ext):
                _ = load_ext(db)
        db.row_factory = sqlite3.Row
        return db

    def get_sqlite_connection(self) -> sqlite3.Connection:
        """Public connection accessor for portable archive import/export."""
        return self._get_connection()

    @staticmethod
    def has_sqlite_vec() -> bool:
        """Check if the sqlite vec condition is satisfied.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        return _sqlite_vec_module is not None

    async def tenant_connection(
        self, tenant_id: str, user_id: str | None = None
    ) -> AsyncContextManager[sqlite3.Connection]:
        """Yield a SQLite connection (no RLS — tenant scope is application-level)."""
        _ = tenant_id, user_id

        @asynccontextmanager
        async def _ctx() -> AsyncGenerator[sqlite3.Connection]:
            conn = self._get_connection()
            try:
                yield conn
            finally:
                conn.close()

        return _ctx()


__all__ = ["SqliteConnectionMixin"]
