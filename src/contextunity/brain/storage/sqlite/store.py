"""Composed SQLite-Vec storage backend for local Brain.
Assembles domain mixins into a single class implementing
``BrainStorageProtocol``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from contextunity.core import get_contextunit_logger

from .blackboard import BlackboardMixin
from .connection import SqliteConnectionMixin
from .episodes import EpisodesMixin
from .facts import FactsMixin
from .graph import GraphMixin
from .schema import SCHEMA_VERSION, apply_preflight_renames, build_core_ddl, build_vector_ddl
from .search import SearchMixin
from .synapses import SynapsesMixin
from .taxonomy import TaxonomyMixin
from .traces import TracesMixin

logger = get_contextunit_logger(__name__)


class SqliteBrainStore(
    GraphMixin,
    EpisodesMixin,
    FactsMixin,
    TracesMixin,
    TaxonomyMixin,
    SearchMixin,
    BlackboardMixin,
    SynapsesMixin,
    SqliteConnectionMixin,
):
    """Local SQLite backend for Brain Service.

    Mirrors ``PostgresBrainStore`` mixin composition.
    Uses sqlite-vec for vector similarity when available.
    """

    db_path: Path
    vector_dim: int

    def __init__(
        self,
        db_path: str = "~/.contextunity/brain_local.sqlite3",
        vector_dim: int = 1536,
    ):
        """Initialize a new instance of SqliteBrainStore.

        Args:
            db_path (str): The db path parameter.
            vector_dim (int): The vector dim parameter.
        """
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.vector_dim = vector_dim
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        if not self.has_sqlite_vec():
            logger.warning(
                (
                    "sqlite-vec not installed — vector search disabled. "
                    "Install with: pip install sqlite-vec"
                )
            )

        with self._get_connection() as db:
            apply_preflight_renames(db)

            for stmt in build_core_ddl():
                _ = db.execute(stmt)

            if self.has_sqlite_vec():
                for stmt in build_vector_ddl(self.vector_dim):
                    try:
                        _ = db.execute(stmt)
                    except sqlite3.OperationalError as e:
                        logger.debug("Vector table init: %s", e)

            # Track schema version
            _ = db.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            db.commit()

        logger.info(
            "SQLite backend ready: %s (schema_v%d, vec=%s)",
            self.db_path,
            SCHEMA_VERSION,
            self.has_sqlite_vec(),
        )

    async def ensure_schema(
        self, *, include_commerce: bool = False, vector_dim: int = 1536
    ) -> None:
        """Schema is ensured on ``__init__``."""
        _ = include_commerce, vector_dim
        logger.info("SQLite schema already initialized at %s", self.db_path)

    async def close(self) -> None:
        """No-op — SQLite connections are per-call."""
        pass


__all__ = ["SqliteBrainStore"]
