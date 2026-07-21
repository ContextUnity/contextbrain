"""Agent traces storage (SQLite implementation).

Contract-compatible with ``postgres/store/traces.py``.
"""

from __future__ import annotations

import sqlite3
import uuid
from hashlib import sha256
from json import dumps as canonical_dumps

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_int, as_str_list
from contextunity.core.types import JsonDict, JsonValue, is_json_dict, is_object_list

from contextunity.brain.core.exceptions import BrainValidationError

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
                INSERT INTO execution_traces
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

    async def finalize_execution_trace(self, *, terminal_trace: JsonDict) -> JsonDict:
        """Durably create or deduplicate one terminal graph-run trace."""
        canonical = dict(terminal_trace)
        supplied_digest = canonical.pop("digest", None)
        computed_digest = sha256(
            canonical_dumps(
                canonical,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if supplied_digest != computed_digest:
            raise BrainValidationError("terminal trace digest mismatch")
        trace_id = str(terminal_trace["trace_id"])
        graph_run_id = str(terminal_trace["graph_run_id"])
        tenant_id = str(terminal_trace["tenant_id"])
        with self._get_connection() as db:
            existing = db.execute(
                """
                SELECT id, payload_digest FROM execution_traces
                WHERE tenant_id = ? AND graph_run_id = ?
                """,
                (tenant_id, graph_run_id),
            ).fetchone()
            if existing is not None:
                existing_id = str(existing[0])
                existing_digest = str(existing[1])
                if existing_digest != computed_digest:
                    raise BrainValidationError("conflicting terminal trace finalization")
                return {
                    "trace_id": existing_id,
                    "graph_run_id": graph_run_id,
                    "digest": computed_digest,
                    "outcome": "duplicate",
                }
            usage = terminal_trace["usage"]
            if not is_json_dict(usage):
                raise BrainValidationError("terminal trace usage is malformed")
            _ = db.execute(
                """
                INSERT INTO execution_traces
                    (id, tenant_id, agent_id, session_id, user_id, graph_name,
                     token_usage, timing_ms, security_flags, metadata, provenance,
                     graph_run_id, payload_digest, terminal_status, terminal_reason,
                     trace_schema_version, prompt_evidence, steps, control_evidence, final_verdict)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    tenant_id,
                    str(terminal_trace["agent_id"]),
                    terminal_trace.get("session_id"),
                    terminal_trace.get("user_id"),
                    str(terminal_trace["graph_name"]),
                    json_dumps(usage),
                    as_int(terminal_trace["duration_ms"]),
                    json_dumps({"codes": terminal_trace.get("security_flags", [])}),
                    json_dumps(
                        {
                            "project_id": terminal_trace["project_id"],
                            "registration_hash": terminal_trace.get("registration_hash"),
                            "plan_id": terminal_trace.get("plan_id"),
                            "plan_revision": terminal_trace.get("plan_revision"),
                            "parent_plan_id": terminal_trace.get("parent_plan_id"),
                            "parent_plan_revision": terminal_trace.get("parent_plan_revision"),
                            "replan_ref": terminal_trace.get("replan_ref"),
                        }
                    ),
                    json_dumps(terminal_trace.get("provenance", [])),
                    graph_run_id,
                    computed_digest,
                    str(terminal_trace["terminal_status"]),
                    str(terminal_trace["terminal_reason"]),
                    str(terminal_trace["schema_version"]),
                    json_dumps(terminal_trace.get("prompt_evidence", [])),
                    json_dumps(terminal_trace.get("steps", [])),
                    json_dumps(terminal_trace.get("control_evidence", {})),
                    json_dumps(terminal_trace.get("final_verdict", {})),
                ),
            )
            db.commit()
        return {
            "trace_id": trace_id,
            "graph_run_id": graph_run_id,
            "digest": computed_digest,
            "outcome": "created",
        }

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
                       security_flags, metadata, provenance, created_at,
                       graph_run_id, payload_digest, terminal_status,
                       terminal_reason, trace_schema_version, prompt_evidence, steps,
                       control_evidence, final_verdict
                FROM execution_traces
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
            for key in (
                "tool_calls",
                "token_usage",
                "security_flags",
                "metadata",
                "prompt_evidence",
                "steps",
                "control_evidence",
                "final_verdict",
            ):
                raw_val = entry.get(key)
                parsed = json_loads(raw_val if isinstance(raw_val, str) else None)
                if key in ("tool_calls", "prompt_evidence", "steps") and is_object_list(parsed):
                    tool_calls: list[JsonValue] = [item for item in parsed if is_json_dict(item)]
                    entry[key] = tool_calls
                elif key == "security_flags" and is_object_list(parsed):
                    entry[key] = [item for item in parsed if isinstance(item, str)]
                elif is_json_dict(parsed):
                    entry[key] = parsed
                else:
                    entry[key] = [] if key in ("tool_calls", "prompt_evidence", "steps") else {}
            prov = entry.get("provenance")
            if isinstance(prov, str):
                prov_parsed = json_loads(prov)
                provenance: list[JsonValue] = [s for s in as_str_list(prov_parsed)]
                entry["provenance"] = provenance
            results.append(entry)

        return results

    async def delete_old_execution_traces(
        self, *, tenant_id: str, older_than_days: int = 30
    ) -> int:
        """Delete only this tenant's traces older than the age threshold."""
        with self._get_connection() as db:
            cursor = db.execute(
                """
                DELETE FROM execution_traces
                WHERE tenant_id = ?
                  AND created_at < datetime('now', ? || ' days')
                """,
                (tenant_id, f"-{older_than_days}"),
            )
            db.commit()
            return cursor.rowcount or 0


__all__ = ["TracesMixin"]
