"""Phase 3 M01: fail-closed guard before removing legacy ``user_facts``."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from psycopg.rows import dict_row

from contextunity.brain.core.exceptions import BrainValidationError

if TYPE_CHECKING:
    from psycopg import AsyncConnection


def guard_and_drop_sqlite_user_facts(db: sqlite3.Connection) -> None:
    """Drop empty legacy ``user_facts`` or fail closed if rows remain."""
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='user_facts'"
    ).fetchone()
    if row is None:
        return
    count_row = db.execute("SELECT COUNT(*) AS n FROM user_facts").fetchone()
    count = int(count_row["n"]) if count_row is not None else 0
    if count > 0:
        raise BrainValidationError(
            message=(
                f"user_facts migration guard: {count} legacy row(s) exist; "
                "migrate to cells before Phase 3 M01 removal."
            ),
        )
    db.execute("DROP TABLE user_facts")


async def guard_and_drop_postgres_user_facts(conn: AsyncConnection) -> None:
    """Drop empty legacy ``user_facts`` or fail closed if rows remain."""
    exists = await conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'user_facts'
        """
    )
    if await exists.fetchone() is None:
        return
    count_cur = conn.cursor(row_factory=dict_row)
    await count_cur.execute("SELECT COUNT(*)::bigint AS n FROM user_facts")
    count_row = await count_cur.fetchone()
    count = int(count_row["n"]) if count_row is not None else 0
    if count > 0:
        raise BrainValidationError(
            message=(
                f"user_facts migration guard: {count} legacy row(s) exist; "
                "migrate to cells before Phase 3 M01 removal."
            ),
        )
    await conn.execute("DROP TABLE user_facts")


__all__ = ["guard_and_drop_sqlite_user_facts", "guard_and_drop_postgres_user_facts"]
