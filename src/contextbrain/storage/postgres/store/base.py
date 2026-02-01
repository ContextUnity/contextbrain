"""Base class with connection pool management and schema isolation."""

from __future__ import annotations

import logging
import os

from psycopg_pool import AsyncConnectionPool

from ..models import KnowledgeStoreInterface

logger = logging.getLogger(__name__)


class PostgresStoreBase(KnowledgeStoreInterface):
    """Base PostgreSQL store with connection pool and schema isolation.

    Supports schema isolation for unified database deployments where
    multiple services (Brain, Commerce) share the same PostgreSQL instance.

    Environment:
        BRAIN_SCHEMA: Schema name (default: 'brain')
    """

    def __init__(
        self,
        *,
        dsn: str,
        pool_min_size: int = 5,
        pool_max_size: int = 20,
        schema: str | None = None,
    ):
        self._dsn = dsn
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._schema = schema or os.getenv("BRAIN_SCHEMA", "brain")
        self._pool: AsyncConnectionPool | None = None

    async def _get_pool(self) -> AsyncConnectionPool:
        """Get or create async connection pool."""
        if self._pool is None or self._pool.closed:
            self._pool = AsyncConnectionPool(
                self._dsn,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                timeout=60.0,
                open=False,
                configure=self._configure_connection,
            )
        if not self._pool._opened:
            await self._pool.open()
        return self._pool

    async def _configure_connection(self, conn):
        """Configure each connection from the pool.

        Sets the search_path to use Brain schema + public (for extensions).
        This ensures schema isolation in unified database deployments.
        """
        await conn.execute(f"SET search_path TO {self._schema}, public")
        logger.debug(f"Connection configured with schema: {self._schema}")

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool and not self._pool.closed:
            await self._pool.close()

    @property
    def schema(self) -> str:
        """Current schema name."""
        return self._schema
