"""SQLite implementations for Brain Admin RPCs (local ``SqliteVecStorageBackend``)."""

from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import UTC, datetime, timedelta

from contextunity.core.narrowing import as_int, as_json_dict_list, as_str, as_str_list
from contextunity.core.types import JsonDict, JsonValue, is_json_dict, is_object_list

from .codecs import json_loads
from .store import SqliteVecStorageBackend
from .traces import sqlite_row_to_json_dict


def _parse_trace_row(row: sqlite3.Row) -> JsonDict:
    entry = sqlite_row_to_json_dict(row)
    for key in ("tool_calls", "token_usage", "security_flags", "metadata"):
        raw_val = entry.get(key)
        parsed = json_loads(raw_val if isinstance(raw_val, str) else None)
        if key == "tool_calls" and is_object_list(parsed):
            entry[key] = [item for item in parsed if is_json_dict(item)]
        elif is_json_dict(parsed):
            entry[key] = parsed
        else:
            entry[key] = [] if key == "tool_calls" else {}
    prov = entry.get("provenance")
    if isinstance(prov, str):
        prov_parsed = json_loads(prov)
        entry["provenance"] = [s for s in as_str_list(prov_parsed)]
    return entry


def _trace_payload(row: JsonDict) -> JsonDict:
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
        "metadata": row.get("metadata") or {},
        "provenance": row.get("provenance") or [],
        "created_at": str(row.get("created_at") or ""),
    }


class SqliteAdminOps:
    """Admin observability queries against the local SQLite Brain backend."""

    def __init__(self, storage: SqliteVecStorageBackend) -> None:
        self._storage = storage

    def list_tenants(self) -> list[JsonDict]:
        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                """
                SELECT tenant_id, COUNT(*) AS trace_count
                FROM agent_traces
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
        if hours is not None:
            conditions.append("created_at > datetime('now', ?)")
            params.append(f"-{int(hours)} hours")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            count_row = db.execute(
                f"SELECT COUNT(*) AS total FROM agent_traces {where}",
                params,
            ).fetchone()
            total = as_int(sqlite_row_to_json_dict(count_row).get("total")) if count_row else 0

            query_params = [*params, limit, offset]
            cursor = db.execute(
                f"""
                SELECT id, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at
                FROM agent_traces
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
                       security_flags, metadata, provenance, created_at
                FROM agent_traces
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
                    FROM agent_traces
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
                       security_flags, metadata, provenance, created_at
                FROM agent_traces
                {where}
                ORDER BY created_at DESC
                LIMIT 200
                """,
                params,
            )
            rows = list(cursor.fetchall())
        return [_trace_payload(_parse_trace_row(row)) for row in rows]

    def get_related_episodes(self, trace_id: str) -> list[JsonDict]:
        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                """
                SELECT e.id, e.tenant_id, e.user_id, e.session_id,
                       e.content, e.metadata, e.created_at
                FROM episodic_events e
                JOIN agent_traces t ON t.tenant_id = e.tenant_id
                WHERE t.id = ?
                  AND (
                      json_extract(e.metadata, '$.trace_id') = ?
                      OR abs(
                          (julianday(t.created_at) - julianday(e.created_at)) * 86400.0
                      ) < 2
                  )
                ORDER BY e.created_at DESC
                """,
                (trace_id, trace_id),
            )
            rows = list(cursor.fetchall())
        return [_episode_payload(sqlite_row_to_json_dict(row)) for row in rows]

    def get_trace_tenant(self, trace_id: str) -> str | None:
        with self._storage.get_sqlite_connection() as db:
            row = db.execute(
                "SELECT tenant_id FROM agent_traces WHERE id = ? LIMIT 1",
                (trace_id,),
            ).fetchone()
        if row is None:
            return None
        return str(sqlite_row_to_json_dict(row).get("tenant_id") or "") or None

    def search_episodes(
        self,
        *,
        tenant_id: str | None,
        user_id: str | None,
        session_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)
        if hours is not None:
            conditions.append("created_at > datetime('now', ?)")
            params.append(f"-{int(hours)} hours")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            count_row = db.execute(
                f"SELECT COUNT(*) AS total FROM episodic_events {where}",
                params,
            ).fetchone()
            total = as_int(sqlite_row_to_json_dict(count_row).get("total")) if count_row else 0
            cursor = db.execute(
                f"""
                SELECT id, tenant_id, user_id, session_id, content, metadata, created_at
                FROM episodic_events
                {where}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            )
            rows = list(cursor.fetchall())
        events = [_episode_payload(sqlite_row_to_json_dict(row)) for row in rows]
        return events, total

    def get_knowledge_nodes(
        self,
        *,
        tenant_id: str | None,
        kind: str | None,
        limit: int,
    ) -> list[JsonDict]:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if kind:
            conditions.append("node_kind = ?")
            params.append(kind)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                f"""
                SELECT id, node_kind, source_type, title,
                       SUBSTR(content, 1, 200) AS content_preview,
                       tenant_id, created_at
                FROM knowledge_nodes
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [*params, limit],
            )
            rows = list(cursor.fetchall())

        return [
            {
                "id": str(sqlite_row_to_json_dict(row).get("id") or ""),
                "node_kind": str(sqlite_row_to_json_dict(row).get("node_kind") or ""),
                "source_type": str(sqlite_row_to_json_dict(row).get("source_type") or ""),
                "title": str(sqlite_row_to_json_dict(row).get("title") or ""),
                "content_preview": str(sqlite_row_to_json_dict(row).get("content_preview") or ""),
                "tenant_id": str(sqlite_row_to_json_dict(row).get("tenant_id") or ""),
                "created_at": str(sqlite_row_to_json_dict(row).get("created_at") or ""),
            }
            for row in rows
        ]

    def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict:
        ep_where = ""
        kn_where = ""
        params: list[object] = []
        if tenant_id:
            ep_where = "WHERE tenant_id = ?"
            kn_where = "WHERE tenant_id = ?"
            params = [tenant_id]

        with self._storage.get_sqlite_connection() as db:
            ep_row = db.execute(
                f"SELECT COUNT(*) AS total FROM episodic_events {ep_where}",
                params,
            ).fetchone()
            kn_row = db.execute(
                f"SELECT COUNT(*) AS total FROM knowledge_nodes {kn_where}",
                params,
            ).fetchone()

        episode_count = as_int(sqlite_row_to_json_dict(ep_row).get("total")) if ep_row else 0
        knowledge_count = as_int(sqlite_row_to_json_dict(kn_row).get("total")) if kn_row else 0
        return {
            "episodes": {"count": episode_count},
            "knowledge_nodes": {"count": knowledge_count},
        }

    def get_analytics_summary(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if hours is not None:
            conditions.append("created_at > datetime('now', ?)")
            params.append(f"-{int(hours)} hours")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            cursor = db.execute(
                f"""
                SELECT timing_ms, token_usage, tool_calls, security_flags, session_id,
                       user_id, created_at, metadata
                FROM agent_traces
                {where}
                """,
                params,
            )
            rows = [sqlite_row_to_json_dict(r) for r in cursor.fetchall()]

        if not rows:
            return {
                "total_traces": 0,
                "traces_24h": 0,
                "traces_1h": 0,
                "avg_timing_ms": 0,
                "p95_timing_ms": 0,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_tokens": 0,
                "tokens_24h_in": 0,
                "tokens_24h_out": 0,
                "tokens_24h_total": 0,
                "unique_sessions": 0,
                "unique_users": 0,
                "tool_usage": {},
                "traces_per_hour": [],
                "security_event_count": 0,
                "total_cost": 0.0,
                "cost_24h_total": 0.0,
            }

        now = datetime.now(UTC)
        cutoff_24h = now - timedelta(hours=24)
        cutoff_1h = now - timedelta(hours=1)

        timings: list[int] = []
        total_in = 0
        total_out = 0
        tokens_24h_in = 0
        tokens_24h_out = 0
        total_cost = 0.0
        cost_24h = 0.0
        sessions: set[str] = set()
        users: set[str] = set()
        tool_counter: Counter[str] = Counter()
        hour_counter: Counter[str] = Counter()
        traces_24h = 0
        traces_1h = 0
        security_events = 0

        for row in rows:
            created_raw = str(row.get("created_at") or "")
            created_at = _parse_sqlite_ts(created_raw)
            in_24h = created_at is not None and created_at >= cutoff_24h
            in_1h = created_at is not None and created_at >= cutoff_1h
            if in_24h:
                traces_24h += 1
            if in_1h:
                traces_1h += 1

            timing = row.get("timing_ms")
            if isinstance(timing, int):
                timings.append(timing)
            elif isinstance(timing, str) and timing.isdigit():
                timings.append(int(timing))

            usage = _json_dict_field(row.get("token_usage"))
            inp = _int_field(usage.get("input_tokens"))
            out = _int_field(usage.get("output_tokens"))
            if not inp and not out:
                step_inp, step_out, step_total = _token_totals_from_metadata(
                    _json_dict_field(row.get("metadata"))
                )
                if step_inp or step_out:
                    inp, out = step_inp, step_out
                elif step_total:
                    out = step_total
            cost = _float_field(usage.get("total_cost"))
            total_in += inp
            total_out += out
            total_cost += cost
            if in_24h:
                tokens_24h_in += inp
                tokens_24h_out += out
                cost_24h += cost

            session_id = str(row.get("session_id") or "")
            if session_id:
                sessions.add(session_id)
            user_id = str(row.get("user_id") or "")
            if user_id:
                users.add(user_id)

            tool_calls_raw = row.get("tool_calls")
            tools = json_loads(tool_calls_raw if isinstance(tool_calls_raw, str) else None)
            if is_object_list(tools):
                for item in tools:
                    if is_json_dict(item):
                        name = str(item.get("tool") or "")
                        if name:
                            tool_counter[name] += 1

            flags = _json_dict_field(row.get("security_flags"))
            events = flags.get("events")
            if isinstance(events, list) and events:
                security_events += 1

            if in_24h and created_at is not None:
                hour_counter[created_at.strftime("%H:%M")] += 1

        timings.sort()
        p95 = timings[int(len(timings) * 0.95)] if timings else 0
        avg_timing = int(sum(timings) / len(timings)) if timings else 0

        return {
            "total_traces": len(rows),
            "traces_24h": traces_24h,
            "traces_1h": traces_1h,
            "avg_timing_ms": avg_timing,
            "p95_timing_ms": p95,
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_tokens": total_in + total_out,
            "tokens_24h_in": tokens_24h_in,
            "tokens_24h_out": tokens_24h_out,
            "tokens_24h_total": tokens_24h_in + tokens_24h_out,
            "unique_sessions": len(sessions),
            "unique_users": len(users),
            "tool_usage": dict(tool_counter.most_common(10)),
            "traces_per_hour": [
                {"hour": hour, "count": count} for hour, count in sorted(hour_counter.items())
            ],
            "security_event_count": security_events,
            "total_cost": total_cost,
            "cost_24h_total": cost_24h,
        }

    def get_system_analytics(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if hours is not None:
            conditions.append("created_at > datetime('now', ?)")
            params.append(f"-{int(hours)} hours")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            row = db.execute(
                f"""
                SELECT
                    COUNT(*) AS total_traces,
                    CAST(AVG(timing_ms) AS INTEGER) AS avg_timing_ms,
                    COUNT(DISTINCT tenant_id) AS unique_tenants,
                    COUNT(DISTINCT session_id) AS unique_sessions,
                    COUNT(DISTINCT user_id) AS unique_users,
                    COALESCE(
                        SUM(CAST(json_extract(token_usage, '$.input_tokens') AS INTEGER)),
                        0
                    ) AS total_input_tokens,
                    COALESCE(
                        SUM(CAST(json_extract(token_usage, '$.output_tokens') AS INTEGER)),
                        0
                    ) AS total_output_tokens
                FROM agent_traces
                {where}
                """,
                params,
            ).fetchone()

        if row is None:
            return {
                "total_traces": 0,
                "avg_timing_ms": 0,
                "unique_tenants": 0,
                "unique_sessions": 0,
                "unique_users": 0,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
            }

        parsed = sqlite_row_to_json_dict(row)
        return {
            "total_traces": _int_field(parsed.get("total_traces")),
            "avg_timing_ms": _int_field(parsed.get("avg_timing_ms")),
            "unique_tenants": _int_field(parsed.get("unique_tenants")),
            "unique_sessions": _int_field(parsed.get("unique_sessions")),
            "unique_users": _int_field(parsed.get("unique_users")),
            "total_input_tokens": _int_field(parsed.get("total_input_tokens")),
            "total_output_tokens": _int_field(parsed.get("total_output_tokens")),
        }


class AsyncSqliteAdminOps:
    """Async ``AdminQueryProtocol`` wrapper over sync ``SqliteAdminOps``."""

    def __init__(self, storage: SqliteVecStorageBackend) -> None:
        self._ops = SqliteAdminOps(storage)

    async def list_tenants(self) -> list[JsonDict]:
        return self._ops.list_tenants()

    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        return self._ops.search_traces(
            tenant_id=tenant_id,
            agent_id=agent_id,
            hours=hours,
            limit=limit,
            offset=offset,
        )

    async def get_trace_details(self, trace_id: str) -> JsonDict | None:
        return self._ops.get_trace_details(trace_id)

    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict:
        return self._ops.get_filter_options(tenant_id=tenant_id)

    async def get_session_traces(self, *, session_id: str, tenant_id: str | None) -> list[JsonDict]:
        return self._ops.get_session_traces(session_id=session_id, tenant_id=tenant_id)

    async def get_related_episodes(self, trace_id: str) -> list[JsonDict]:
        return self._ops.get_related_episodes(trace_id)

    async def get_trace_tenant(self, trace_id: str) -> str | None:
        return self._ops.get_trace_tenant(trace_id)

    async def search_episodes(
        self,
        *,
        tenant_id: str | None,
        user_id: str | None,
        session_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        return self._ops.search_episodes(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            hours=hours,
            limit=limit,
            offset=offset,
        )

    async def get_knowledge_nodes(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]:
        return self._ops.get_knowledge_nodes(tenant_id=tenant_id, kind=kind, limit=limit)

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict:
        return self._ops.get_memory_layer_stats(tenant_id=tenant_id)

    async def get_analytics_summary(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        return self._ops.get_analytics_summary(tenant_id=tenant_id, hours=hours)

    async def get_system_analytics(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        return self._ops.get_system_analytics(tenant_id=tenant_id, hours=hours)


def _episode_payload(row: JsonDict) -> JsonDict:
    return {
        "id": str(row.get("id") or ""),
        "tenant_id": str(row.get("tenant_id") or ""),
        "user_id": str(row.get("user_id") or ""),
        "session_id": str(row.get("session_id") or ""),
        "content": str(row.get("content") or ""),
        "metadata": _json_dict_field(row.get("metadata")),
        "created_at": str(row.get("created_at") or ""),
    }


def _token_totals_from_metadata(meta: JsonDict) -> tuple[int, int, int]:
    """Resolve token counts from ``metadata.steps`` when ``token_usage`` is empty."""
    steps = as_json_dict_list(meta.get("steps"))
    if not steps:
        return 0, 0, 0

    total_in = 0
    total_out = 0
    group_total = 0

    def walk(step_list: list[JsonDict]) -> None:
        nonlocal total_in, total_out
        for step in step_list:
            tin = as_int(step.get("tokens_in"))
            tout = as_int(step.get("tokens_out"))
            if tin or tout:
                total_in += tin
                total_out += tout
            children = as_json_dict_list(step.get("children"))
            if children:
                walk(children)

    walk(steps)
    if total_in or total_out:
        return total_in, total_out, total_in + total_out

    for step in steps:
        if step.get("is_group"):
            cum = as_int(step.get("cumulative_tokens"))
            if cum:
                group_total += cum
        else:
            tin = as_int(step.get("tokens_in"))
            tout = as_int(step.get("tokens_out"))
            tok = as_int(step.get("tokens")) if isinstance(step.get("tokens"), (int, float)) else 0
            group_total += tin + tout + tok
    return 0, 0, group_total


def _json_dict_field(raw: object) -> JsonDict:
    if is_json_dict(raw):
        return raw
    if isinstance(raw, str):
        parsed = json_loads(raw)
        if is_json_dict(parsed):
            return parsed
    return {}


def _int_field(raw: object) -> int:
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return 0


def _float_field(raw: object) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        try:
            return float(raw)
        except ValueError:
            return 0.0
    return 0.0


def _parse_sqlite_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(raw.replace("Z", ""), fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


__all__ = ["SqliteAdminOps"]
