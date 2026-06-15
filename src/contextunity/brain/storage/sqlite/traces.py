"""Agent traces storage (SQLite implementation).

Contract-compatible with ``postgres/store/traces.py``.
"""

from __future__ import annotations

import sqlite3
import uuid

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str_list
from contextunity.core.types import JsonDict, JsonValue, is_json_dict, is_object_list

from .codecs import json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin

logger = get_contextunit_logger(__name__)


def _sqlite_cell(value: object) -> JsonValue:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def sqlite_row_to_json_dict(row: sqlite3.Row) -> JsonDict:
    return {key: _sqlite_cell(sqlite_cell(row, key)) for key in row.keys()}


class TracesMixin(SqliteConnectionMixin):
    """SQLite trace operations matching Postgres contract."""

    async def log_trace(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        session_id: str | None = None,
        user_id: str | None = None,
        graph_name: str | None = None,
        tool_calls: list[JsonDict] | None = None,
        token_usage: JsonDict | None = None,
        timing_ms: int | None = None,
        security_flags: JsonDict | None = None,
        metadata: JsonDict | None = None,
        provenance: list[str] | None = None,
    ) -> str:
        """Log an agent execution trace. Returns generated trace UUID."""
        trace_id = str(uuid.uuid4())

        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO agent_traces
                    (id, tenant_id, agent_id, session_id, user_id, graph_name,
                     tool_calls, token_usage, timing_ms, security_flags,
                     metadata, provenance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    tenant_id,
                    agent_id,
                    session_id,
                    user_id,
                    graph_name,
                    json_dumps(tool_calls or []),
                    json_dumps(token_usage or {}),
                    timing_ms,
                    json_dumps(security_flags or {}),
                    json_dumps(metadata or {}),
                    json_dumps(provenance),
                ),
            )
            db.commit()

        return trace_id

    async def get_traces(
        self,
        *,
        tenant_id: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        limit: int = 20,
        since: str | None = None,
    ) -> list[JsonDict]:
        """Get agent traces with optional filters."""
        conditions = ["tenant_id = ?"]
        params: list[object] = [tenant_id]

        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if agent_id:
            conditions.append("agent_id = ?")
            params.append(agent_id)
        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)
        if since:
            conditions.append("created_at > ?")
            params.append(since)

        where = " AND ".join(conditions)
        params.append(limit)

        with self._get_connection() as db:
            cursor = db.execute(
                f"""
                SELECT id, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at
                FROM agent_traces
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                params,
            )
            rows: list[sqlite3.Row] = list(cursor.fetchall())

        results: list[JsonDict] = []
        for row in rows:
            entry = sqlite_row_to_json_dict(row)
            for key in ("tool_calls", "token_usage", "security_flags", "metadata"):
                raw_val = entry.get(key)
                parsed = json_loads(raw_val if isinstance(raw_val, str) else None)
                if key == "tool_calls" and is_object_list(parsed):
                    tool_calls: list[JsonValue] = [item for item in parsed if is_json_dict(item)]
                    entry[key] = tool_calls
                elif is_json_dict(parsed):
                    entry[key] = parsed
                else:
                    entry[key] = [] if key == "tool_calls" else {}
            prov = entry.get("provenance")
            if isinstance(prov, str):
                prov_parsed = json_loads(prov)
                provenance: list[JsonValue] = [s for s in as_str_list(prov_parsed)]
                entry["provenance"] = provenance
            results.append(entry)

        return results


__all__ = ["TracesMixin"]
