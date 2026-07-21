"""SQLite analytics queries for Brain Admin RPCs."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

from contextunity.core.narrowing import as_int, as_json_dict_list
from contextunity.core.types import JsonDict, is_json_dict, is_object_list

from .admin_payloads import _json_dict_field
from .codecs import json_loads
from .store import SqliteBrainStore
from .traces import sqlite_row_to_json_dict


class _SqliteAnalyticsAdminOpsMixin:
    _storage: SqliteBrainStore

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
                FROM execution_traces
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
                FROM execution_traces
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


__all__ = ["_SqliteAnalyticsAdminOpsMixin"]
