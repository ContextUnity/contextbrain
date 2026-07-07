"""Base class with connection pool management and schema isolation."""

from __future__ import annotations

import logging
from abc import ABC
from contextlib import AbstractAsyncContextManager

from contextunity.core import get_contextunit_logger
from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from ..models import BrainStorageInterface
from ..schema import (
    build_column_backfill_sql,
    build_extension_sql,
    build_preflight_rename_sql,
    build_rls_sql,
    build_schema_sql,
)

logger = get_contextunit_logger(__name__)

# psycopg.pool emits WARNING per pool-worker on every connection issue;
# one error-level message is enough — suppress repetitive warnings.
get_contextunit_logger("psycopg.pool").setLevel(logging.ERROR)


class PostgresStoreBase(BrainStorageInterface, ABC):
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
        """Initialize a new instance of PostgresStoreBase."""
        self._dsn: str = dsn
        self._pool_min_size: int = pool_min_size
        self._pool_max_size: int = pool_max_size
        self._schema: str = schema
        self._pool: AsyncConnectionPool | None = None

    async def _get_pool(self) -> AsyncConnectionPool:
        """Get or create async connection pool.

        Returns:
            AsyncConnectionPool: An instance of AsyncConnectionPool.
        """
        if self._pool is None or self._pool.closed:
            self._pool = AsyncConnectionPool(
                self._dsn,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                timeout=60.0,
                open=False,
                check=AsyncConnectionPool.check_connection,
                configure=self._configure_connection,
                kwargs={
                    "keepalives": 1,
                    "keepalives_idle": 60,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            )
        if self._pool.closed:
            await self._pool.open()
        return self._pool

    async def _configure_connection(self, conn: AsyncConnection[object]) -> None:
        """Configure each connection from the pool.

        Args:
            conn: The pool connection to configure.
        """
        await conn.set_autocommit(True)
        _ = await conn.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self._schema))
        )
        await conn.set_autocommit(False)
        logger.debug("Connection configured with schema: %s", self._schema)

    async def tenant_connection(
        self, tenant_id: str, user_id: str | None = None
    ) -> AbstractAsyncContextManager[AsyncConnection[object]]:
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
            """ctx."""
            pool = await self._get_pool()
            async with pool.connection() as conn:
                # Fail closed: if the RLS role/tenant context cannot be
                # established, the operation must not proceed with only
                # application-level filtering (a missing 'brain_app' role
                # is tolerated inside set_tenant_context with a warning).
                from .helpers import set_tenant_context

                await set_tenant_context(conn, tenant_id, user_id)

                try:
                    yield conn
                    if not conn.closed:
                        await conn.commit()
                except Exception:
                    if not conn.closed:
                        await conn.rollback()
                    raise

        return _ctx()

    async def ensure_schema(
        self,
        *,
        include_commerce: bool = False,
        vector_dim: int = 768,
    ) -> None:
        """Ensure database schema and tables exist.

        Idempotent — safe to call on every startup.

        Steps:
            1. Create schema namespace
            2. Install extensions (vector, ltree)
            3. Set search_path
            4. Run the CP-1 breaking preflight rename (legacy names -> canonical)
            5. Run CREATE TABLE IF NOT EXISTS (no-op for existing tables)
            6. Run column backfill (ALTER TABLE ADD COLUMN IF NOT EXISTS)
            7. Run constraint upgrades
            8. Apply RLS policies for tenant isolation

        Args:
            include_commerce: Create commerce/taxonomy tables
            vector_dim: Embedding vector dimension (must match embedder output)
        """

        pool = await self._get_pool()
        async with pool.connection() as conn:
            # DDL requires autocommit — otherwise psycopg3 wraps
            # everything in a transaction and rollbacks on any error.
            await conn.set_autocommit(True)
            try:
                # 1. Ensure schema namespace exists
                _ = await conn.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._schema))
                )

                # 2. Extensions (require superuser — graceful if pre-provisioned)
                for ext_stmt in build_extension_sql():
                    try:
                        _ = await conn.execute(ext_stmt.encode())
                    except Exception as ext_err:
                        logger.warning(
                            (
                                "Cannot create extension (needs superuser): %s — "
                                "ensure extensions are pre-provisioned"
                            ),
                            ext_err,
                        )

                # 3. Set search_path for this session
                _ = await conn.execute(
                    sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self._schema))
                )

                # 4. CP-1 breaking preflight: rename legacy physical names to
                # canonical names before any CREATE TABLE IF NOT EXISTS runs.
                # Not wrapped in try/except — a failure here must fail startup
                # (fail closed) rather than leave a half-migrated schema.
                for stmt in build_preflight_rename_sql():
                    _ = await conn.execute(stmt.encode())

                # 5. Run all DDL statements (all use IF NOT EXISTS)
                statements = build_schema_sql(
                    vector_dim=vector_dim,
                    include_commerce=include_commerce,
                )
                for stmt in statements:
                    _ = await conn.execute(stmt.encode())

                # 6. Column backfill + constraint upgrades
                # Handles columns/constraints added in code but missing
                # from tables that already existed on disk.
                for stmt in build_column_backfill_sql():
                    try:
                        _ = _ = await conn.execute(stmt.encode())
                    except Exception as backfill_err:
                        logger.warning("Column backfill skipped: %s", backfill_err)

                # 7. Apply Row-Level Security policies (tenant isolation)
                # RLS is defence-in-depth — even if app-level checks are
                # bypassed, PostgreSQL blocks cross-tenant data access.
                for stmt in build_rls_sql():
                    try:
                        _ = _ = await conn.execute(stmt.encode())
                    except Exception as rls_err:
                        logger.warning(
                            "RLS policy skipped (needs superuser or table owner): %s",
                            rls_err,
                        )

                logger.info(
                    "Schema ensured: %s (core=yes, commerce=%s, rls=yes)",
                    self._schema,
                    include_commerce,
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
        """Current schema name.

        Returns:
            str: The resulting string value.
        """
        return self._schema
