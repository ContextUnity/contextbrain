"""Base class with connection pool management and schema isolation."""

from __future__ import annotations

import logging

from contextcore import get_context_unit_logger
from psycopg_pool import AsyncConnectionPool

from ..models import KnowledgeStoreInterface
from ..schema import build_column_backfill_sql, build_extension_sql, build_rls_sql, build_schema_sql

logger = get_context_unit_logger(__name__)

# psycopg.pool emits WARNING per pool-worker on every connection issue;
# one error-level message is enough — suppress repetitive warnings.
logging.getLogger("psycopg.pool").setLevel(logging.ERROR)


class PostgresStoreBase(KnowledgeStoreInterface):
    """Base PostgreSQL store with connection pool and schema isolation.

    Supports schema isolation for unified database deployments where
    multiple services (Brain, Commerce) share the same PostgreSQL instance.

    Environment:
        BRAIN_SCHEMA: Schema name (default: 'brain')
        PGVECTOR_DIM: Embedding dimension (default: 1536)
    """

    def __init__(
        self,
        *,
        dsn: str,
        pool_min_size: int = 5,
        pool_max_size: int = 20,
        schema: str = "brain",
    ):
        self._dsn = dsn
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._schema = schema
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

        NOTE: psycopg3 defaults to autocommit=False, so any SQL here opens
        an implicit transaction. The pool expects idle connections after
        configure — use autocommit to avoid INTRANS state.
        """
        await conn.set_autocommit(True)
        await conn.execute(f"SET search_path TO {self._schema}, public")
        await conn.set_autocommit(False)
        logger.debug("Connection configured with schema: %s", self._schema)

    async def tenant_connection(self, tenant_id: str, user_id: str | None = None):
        """Async context manager: pool connection with RLS tenant context.

        Usage::

            async with self.tenant_connection("nszu", "patient_123") as conn:
                await execute(conn, "SELECT ...", params)

        Sets ``app.current_tenant`` and ``app.current_user`` for PostgreSQL RLS policies.
        The setting is transaction-scoped (SET LOCAL via set_config)
        and reverts automatically on COMMIT/ROLLBACK.

        Args:
            tenant_id: Project/tenant ID. Use '*' for admin access.
            user_id: Optional user ID for intra-tenant isolation.
        """
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _ctx():
            pool = await self._get_pool()
            async with pool.connection() as conn:
                try:
                    from .helpers import set_tenant_context

                    await set_tenant_context(conn, tenant_id, user_id)
                except Exception as e:
                    # RLS is defence-in-depth — don't block operations
                    # if set_tenant_context fails.  Application-level
                    # WHERE tenant_id = %s still provides isolation.
                    logger.warning(
                        "Failed to set RLS tenant context for '%s': %s. "
                        "Application-level tenant filtering still active.",
                        tenant_id,
                        e,
                    )
                yield conn

        return _ctx()

    async def ensure_schema(
        self,
        *,
        include_commerce: bool = False,
        include_news_engine: bool = False,
        vector_dim: int = 768,
    ) -> None:
        """Ensure database schema and tables exist.

        Idempotent — safe to call on every startup.

        Steps:
            1. Create schema namespace
            2. Install extensions (vector, ltree)
            3. Set search_path
            4. Run CREATE TABLE IF NOT EXISTS (no-op for existing tables)
            5. Run column backfill (ALTER TABLE ADD COLUMN IF NOT EXISTS)
            6. Run constraint upgrades
            7. Apply RLS policies for tenant isolation

        Args:
            include_commerce: Create commerce/taxonomy tables
            include_news_engine: Create news pipeline tables
            vector_dim: Embedding vector dimension (must match embedder output)
        """

        pool = await self._get_pool()
        async with pool.connection() as conn:
            # DDL requires autocommit — otherwise psycopg3 wraps
            # everything in a transaction and rollbacks on any error.
            await conn.set_autocommit(True)
            try:
                # 1. Ensure schema namespace exists
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

                # 2. Extensions (require superuser — graceful if pre-provisioned)
                for ext_stmt in build_extension_sql():
                    try:
                        await conn.execute(ext_stmt)
                    except Exception as ext_err:
                        logger.warning(
                            "Cannot create extension (needs superuser): %s — "
                            "ensure extensions are pre-provisioned",
                            ext_err,
                        )

                # 3. Set search_path for this session
                await conn.execute(f"SET search_path TO {self._schema}, public")

                # 4. Run all DDL statements (all use IF NOT EXISTS)
                statements = build_schema_sql(
                    vector_dim=vector_dim,
                    include_commerce=include_commerce,
                    include_news_engine=include_news_engine,
                )
                for stmt in statements:
                    await conn.execute(stmt)

                # 5. Column backfill + constraint upgrades
                # Handles columns/constraints added in code but missing
                # from tables that already existed on disk.
                for stmt in build_column_backfill_sql():
                    try:
                        await conn.execute(stmt)
                    except Exception as backfill_err:
                        logger.warning("Column backfill skipped: %s", backfill_err)

                # 6. Apply Row-Level Security policies (tenant isolation)
                # RLS is defence-in-depth — even if app-level checks are
                # bypassed, PostgreSQL blocks cross-tenant data access.
                for stmt in build_rls_sql():
                    try:
                        await conn.execute(stmt)
                    except Exception as rls_err:
                        logger.warning(
                            "RLS policy skipped (needs superuser or table owner): %s",
                            rls_err,
                        )

                logger.info(
                    "Schema ensured: %s (core=yes, commerce=%s, news=%s, rls=yes)",
                    self._schema,
                    include_commerce,
                    include_news_engine,
                )
            except Exception:
                logger.error("Failed to ensure schema '%s'", self._schema, exc_info=True)
                raise
            finally:
                await conn.set_autocommit(False)

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool and not self._pool.closed:
            await self._pool.close()

    @property
    def schema(self) -> str:
        """Current schema name."""
        return self._schema
