"""SQLite connection lifecycle management."""

from __future__ import annotations

import importlib
import sqlite3
import types
from pathlib import Path

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


__all__ = ["SqliteConnectionMixin"]
