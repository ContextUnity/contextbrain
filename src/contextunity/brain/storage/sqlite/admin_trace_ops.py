"""SQLite trace and tenant queries for Brain Admin RPCs."""

from __future__ import annotations

import sqlite3

from contextunity.core.narrowing import as_int, as_str, as_str_list
from contextunity.core.types import JsonDict, JsonValue, is_json_dict, is_object_list

from .codecs import json_loads
from .store import SqliteBrainStore
from .traces import sqlite_row_to_json_dict


def _parse_trace_row(row: sqlite3.Row) -> JsonDict:
    entry = sqlite_row_to_json_dict(row)
    for key in (
        "tool_calls",
        "token_usage",
        "security_flags",
        "metadata",
        "prompt_evidence",
        "steps",
    ):
        raw_val = entry.get(key)
        parsed = json_loads(raw_val if isinstance(raw_val, str) else None)
        if key in ("tool_calls", "prompt_evidence", "steps") and is_object_list(parsed):
            entry[key] = [item for item in parsed if is_json_dict(item)]
        elif is_json_dict(parsed):
            entry[key] = parsed
        else:
            entry[key] = [] if key in ("tool_calls", "prompt_evidence", "steps") else {}
    prov = entry.get("provenance")
    if isinstance(prov, str):
        prov_parsed = json_loads(prov)
        entry["provenance"] = [s for s in as_str_list(prov_parsed)]
    return entry


def _trace_payload(row: JsonDict) -> JsonDict:
    metadata_raw = row.get("metadata")
    metadata: JsonDict = metadata_raw if is_json_dict(metadata_raw) else {}
    return {
        "id": str(row.get("id") or ""),
        "tenant_id": str(row.get("tenant_id") or ""),
        "agent_id": str(row.get("agent_id") or ""),
        "session_id": str(row.get("session_id") or ""),
        "user_id": str(row.get("user_id") or ""),
        "graph_name": str(row.get("graph_name") or ""),
        "tool_calls": row.get("tool_calls") or [],
        "token_usage": row.get("token_usage") or {},
        "timing_ms": row.get("timing_ms"),
        "security_flags": row.get("security_flags") or {},
        "metadata": metadata,
        "project_id": as_str(metadata.get("project_id")),
        "registration_hash": as_str(metadata.get("registration_hash")),
        "provenance": row.get("provenance") or [],
        "graph_run_id": str(row.get("graph_run_id") or ""),
        "payload_digest": str(row.get("payload_digest") or ""),
        "terminal_status": str(row.get("terminal_status") or ""),
        "terminal_reason": str(row.get("terminal_reason") or ""),
        "trace_schema_version": str(row.get("trace_schema_version") or "legacy_v0"),
        "prompt_evidence": row.get("prompt_evidence") or [],
        "steps": row.get("steps") or [],
        "created_at": str(row.get("created_at") or ""),
    }


class _SqliteTraceAdminOpsMixin:
    _storage: SqliteBrainStore

    def list_tenants(self) -> list[JsonDict]:
        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                """
                SELECT tenant_id, COUNT(*) AS trace_count
                FROM execution_traces
                GROUP BY tenant_id
                ORDER BY tenant_id
                """
            )
            rows = list(cursor.fetchall())
        return [
            {
                "id": as_str(sqlite_row_to_json_dict(row).get("tenant_id")),
                "trace_count": as_int(sqlite_row_to_json_dict(row).get("trace_count")),
            }
            for row in rows
            if as_str(sqlite_row_to_json_dict(row).get("tenant_id"))
        ]

    def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        status: str | None = None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if agent_id:
            conditions.append("agent_id = ?")
            params.append(agent_id)
        if status:
            conditions.append("terminal_status = ?")
            params.append(status)
        if hours is not None:
            conditions.append("created_at > datetime('now', ?)")
            params.append(f"-{int(hours)} hours")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            count_row = db.execute(
                f"SELECT COUNT(*) AS total FROM execution_traces {where}",
                params,
            ).fetchone()
            total = as_int(sqlite_row_to_json_dict(count_row).get("total")) if count_row else 0

            query_params = [*params, limit, offset]
            cursor = db.execute(
                f"""
                SELECT id, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at,
                       graph_run_id, payload_digest, terminal_status, terminal_reason,
                       trace_schema_version, prompt_evidence, steps
                FROM execution_traces
                {where}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                query_params,
            )
            rows = list(cursor.fetchall())

        traces = [_trace_payload(_parse_trace_row(row)) for row in rows]
        return traces, total

    def get_trace_details(self, trace_id: str) -> JsonDict | None:
        with self._storage.get_sqlite_connection() as db:
            row = db.execute(
                """
                SELECT id, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at,
                       graph_run_id, payload_digest, terminal_status, terminal_reason,
                       trace_schema_version, prompt_evidence, steps
                FROM execution_traces
                WHERE id = ?
                LIMIT 1
                """,
                (trace_id,),
            ).fetchone()
        if row is None:
            return None
        return _trace_payload(_parse_trace_row(row))

    def get_filter_options(self, *, tenant_id: str | None) -> JsonDict:
        cols = [
            ("agent_id", "agent_ids"),
            ("tenant_id", "tenant_ids"),
            ("graph_name", "graph_names"),
            ("user_id", "user_ids"),
        ]
        tenant_clause = ""
        params: list[object] = []
        if tenant_id:
            tenant_clause = "AND tenant_id = ?"
            params = [tenant_id]

        filter_options: JsonDict = {}
        with self._storage.get_sqlite_connection() as db:
            for col, key in cols:
                cursor = db.execute(
                    f"""
                    SELECT DISTINCT {col} AS value
                    FROM execution_traces
                    WHERE {col} IS NOT NULL AND {col} != '' {tenant_clause}
                    ORDER BY {col}
                    LIMIT 100
                    """,
                    params,
                )
                values: list[JsonValue] = [
                    as_str(sqlite_row_to_json_dict(row).get("value"))
                    for row in cursor.fetchall()
                    if as_str(sqlite_row_to_json_dict(row).get("value"))
                ]
                filter_options[key] = values
        return filter_options

    def get_session_traces(
        self,
        *,
        session_id: str,
        tenant_id: str | None,
    ) -> list[JsonDict]:
        conditions = ["session_id = ?"]
        params: list[object] = [session_id]
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        where = "WHERE " + " AND ".join(conditions)

        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                f"""
                SELECT id, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at,
                       graph_run_id, payload_digest, terminal_status, terminal_reason,
                       trace_schema_version, prompt_evidence, steps
                FROM execution_traces
                {where}
                ORDER BY created_at DESC
                LIMIT 200
                """,
                params,
            )
            rows = list(cursor.fetchall())
        return [_trace_payload(_parse_trace_row(row)) for row in rows]

    def get_trace_tenant(self, trace_id: str) -> str | None:
        with self._storage.get_sqlite_connection() as db:
            row = db.execute(
                "SELECT tenant_id FROM execution_traces WHERE id = ? LIMIT 1",
                (trace_id,),
            ).fetchone()
        if row is None:
            return None
        return str(sqlite_row_to_json_dict(row).get("tenant_id") or "") or None


__all__ = ["_SqliteTraceAdminOpsMixin"]
