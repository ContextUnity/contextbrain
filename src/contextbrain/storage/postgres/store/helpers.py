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


__all__ = ["vec", "execute", "fetch_all", "Json"]
