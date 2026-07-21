"""Connection type shared by Brain storage protocols."""

from __future__ import annotations

import sqlite3

from psycopg import AsyncConnection

# The only two backends implementing this Protocol: Postgres yields a
# psycopg async connection, SQLite yields a stdlib sqlite3 connection. No
# caller inspects the yielded object generically through the Protocol-typed
# `storage` reference (grep-verified) — the union is the honest type, not a
# stand-in for "don't know".
TenantConnection = AsyncConnection[object] | sqlite3.Connection


__all__ = ["TenantConnection"]
