"""Base class with connection pool management."""

from __future__ import annotations

import logging

from psycopg_pool import AsyncConnectionPool

from ..models import KnowledgeStoreInterface

logger = logging.getLogger(__name__)


class PostgresStoreBase(KnowledgeStoreInterface):
    """Base PostgreSQL store with connection pool."""

    def __init__(self, *, dsn: str, pool_min_size: int = 5, pool_max_size: int = 20):
        self._dsn = dsn
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool: AsyncConnectionPool | None = None

    async def _get_pool(self) -> AsyncConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = AsyncConnectionPool(
                self._dsn,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                timeout=60.0,
                open=False,
            )
        if not self._pool._opened:
            await self._pool.open()
        return self._pool

    async def close(self) -> None:
        if self._pool and not self._pool.closed:
            await self._pool.close()
