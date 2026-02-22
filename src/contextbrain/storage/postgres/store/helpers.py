"""Shared helpers for PostgreSQL storage."""

from __future__ import annotations

from typing import Any, List

from psycopg.rows import dict_row
from psycopg.types.json import Json


def vec(v: List[float]) -> str:
    """Format vector for pgvector: [0.1,0.2,0.3]"""
    return "[" + ",".join(f"{float(x):.8f}" for x in v) + "]"


async def execute(conn, query: str, params: dict) -> Any:
    """Execute query with params."""
    return await conn.execute(query, params)


async def fetch_all(conn, query: str, params: dict) -> List[dict]:
    """Execute query and fetch all rows as dicts."""
    conn.row_factory = dict_row
    result = await conn.execute(query, params)
    return await result.fetchall()


async def set_tenant_context(conn, tenant_id: str, user_id: str | None = None) -> None:
    """Set the RLS tenant context for the current transaction.

    Must be called INSIDE a transaction (autocommit=False) so that
    ``SET LOCAL`` scopes the setting to the current transaction only.
    After COMMIT/ROLLBACK the setting reverts automatically.

    Args:
        conn: psycopg async connection (must be in a transaction)
        tenant_id: The tenant/project ID to scope queries to.
            Use '*' for admin/dashboard access (bypasses RLS via policy).
        user_id: Optional user identifier for intra-tenant isolation.
            If None, sets to '*' (bypasses user-level RLS).

    Raises:
        ValueError: If tenant_id is empty (fail-closed — prevents
            accidentally querying without tenant context).
    """
    if not tenant_id:
        raise ValueError(
            "tenant_id is required for RLS context. Pass a valid tenant_id or '*' for admin access."
        )
    # SET LOCAL is transaction-scoped — reverts after COMMIT/ROLLBACK.
    # We use format() here because SET doesn't support $1 parameters,
    # but tenant_id is validated (not empty, no SQL injection risk because
    # it comes from validated payload, not raw user input).
    await conn.execute(
        "SELECT set_config('app.current_tenant', %s, true)",
        [tenant_id],
    )

    # Set app.current_user for fine-grained user-level RLS inside the tenant
    actual_user = user_id if user_id is not None else "*"
    await conn.execute(
        "SELECT set_config('app.current_user', %s, true)",
        [actual_user],
    )


__all__ = ["vec", "execute", "fetch_all", "Json", "set_tenant_context"]
