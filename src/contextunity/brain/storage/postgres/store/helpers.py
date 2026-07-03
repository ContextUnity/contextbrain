"""Shared helpers for PostgreSQL storage."""

from __future__ import annotations

from collections.abc import Mapping

from contextunity.core import get_contextunit_logger
from contextunity.core.types import JsonDict, is_json_dict
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.types.json import Json

from contextunity.brain.core.exceptions import BrainValidationError

logger = get_contextunit_logger(__name__)

type PgConnection = AsyncConnection[object]


def vec(v: list[float]) -> str:
    """Format vector for pgvector: [0.1,0.2,0.3]"""
    return "[" + ",".join(f"{float(x):.8f}" for x in v) + "]"


async def execute(conn: PgConnection, query: str, params: Mapping[str, object]) -> object:
    """Execute query with params."""
    return await conn.execute(query.encode(), params)


async def fetch_all(conn: PgConnection, query: str, params: Mapping[str, object]) -> list[JsonDict]:
    """Execute query and fetch all rows as dicts."""
    cur = conn.cursor(row_factory=dict_row)
    result = await cur.execute(query.encode(), params)
    rows = await result.fetchall()
    return [row for row in rows if is_json_dict(row)]


_ROLE_MISSING_WARNED = False


async def set_tenant_context(
    conn: PgConnection, tenant_id: str | None, user_id: str | None = None
) -> None:
    """Set the RLS execution role and tenant context for the current transaction.

    Must be called INSIDE a transaction (autocommit=False) so that
    ``SET LOCAL`` scopes the setting to the current transaction only.
    After COMMIT/ROLLBACK the setting reverts automatically.

    ``SET LOCAL ROLE brain_app`` drops superuser/owner privileges for the
    transaction so PostgreSQL actually enforces the RLS policies — without
    it a superuser DSN (common in docker-compose) silently bypasses RLS.

    Args:
        conn: psycopg async connection (must be in a transaction)
        tenant_id: The tenant/project ID to scope queries to.
            Use '*' for admin/dashboard access (bypasses RLS via policy).
        user_id: Optional user identifier for intra-tenant isolation.
            If None, sets to '*' (bypasses user-level RLS).

    Raises:
        BrainValidationError: If tenant_id is empty (fail-closed — prevents
            accidentally querying without tenant context).
    """
    global _ROLE_MISSING_WARNED
    if not tenant_id:
        raise BrainValidationError(
            "tenant_id is required for RLS context. Pass a valid tenant_id or '*' for admin access."
        )
    # Transaction-scoped role switch: enforce RLS even for superuser/owner
    # DSNs. Tolerate only a missing role (RLS not provisioned on this DB);
    # any other failure propagates — fail closed.
    try:
        _ = await conn.execute("SET LOCAL ROLE brain_app")
    except errors.UndefinedObject:
        if not _ROLE_MISSING_WARNED:
            logger.warning(
                "Role 'brain_app' does not exist — RLS enforcement degraded to "
                "application-level tenant filters. Run ensure_schema with a "
                "privileged DSN to provision RLS roles/policies."
            )
            _ROLE_MISSING_WARNED = True
    # SET LOCAL is transaction-scoped — reverts after COMMIT/ROLLBACK.
    # We use format() here because SET doesn't support $1 parameters,
    # but tenant_id is validated (not empty, no SQL injection risk because
    # it comes from validated payload, not raw user input).
    _ = await conn.execute(
        "SELECT set_config('app.current_tenant', %s, true)",
        [tenant_id],
    )

    # Set app.current_user for fine-grained user-level RLS inside the tenant
    actual_user = user_id if user_id is not None else "*"
    _ = await conn.execute(
        "SELECT set_config('app.current_user', %s, true)",
        [actual_user],
    )


__all__ = ["PgConnection", "vec", "execute", "fetch_all", "Json", "set_tenant_context"]
