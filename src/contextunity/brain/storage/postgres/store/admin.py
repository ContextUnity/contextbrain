"""PostgreSQL admin observability queries for Brain Admin RPCs."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from contextunity.core.narrowing import as_float, as_int, as_str
from contextunity.core.types import JsonDict, JsonValue

from .helpers import fetch_all


def _row_int(row: JsonDict, key: str) -> int:
    return as_int(row.get(key), default=0)


def _row_float(row: JsonDict, key: str) -> float:
    return as_float(row.get(key), default=0.0)


if TYPE_CHECKING:
    from contextunity.brain.storage.postgres.store import PostgresKnowledgeStore


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


def _episode_payload(row: JsonDict) -> JsonDict:
    return {
        "id": str(row.get("id") or ""),
        "tenant_id": str(row.get("tenant_id") or ""),
        "user_id": str(row.get("user_id") or ""),
        "session_id": str(row.get("session_id") or ""),
        "content": row.get("content") or "",
        "metadata": row.get("metadata") or {},
        "created_at": str(row.get("created_at") or ""),
    }


def _empty_analytics_summary() -> JsonDict:
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


class PostgresAdminOps:
    """Admin observability queries against the PostgreSQL Brain backend."""

    def __init__(self, storage: PostgresKnowledgeStore) -> None:
        self._storage = storage

    async def list_tenants(self) -> list[JsonDict]:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT tenant_id, COUNT(*) AS trace_count
                FROM agent_traces
                GROUP BY tenant_id
                ORDER BY tenant_id
                """,
                {},
            )
        return [
            {
                "id": as_str(row.get("tenant_id"), default=""),
                "trace_count": _row_int(row, "trace_count"),
            }
            for row in rows
            if as_str(row.get("tenant_id"), default="")
        ]

    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        if agent_id:
            conditions.append("agent_id = %(agent_id)s")
            qparams["agent_id"] = agent_id
        if hours is not None:
            conditions.append("created_at > NOW() - make_interval(hours => %(hours)s)")
            qparams["hours"] = hours
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        qparams["limit"] = limit
        qparams["offset"] = offset

        query = f"""
            SELECT id::text, tenant_id, agent_id, session_id, user_id,
                   graph_name, tool_calls, token_usage, timing_ms,
                   security_flags, metadata, provenance, created_at::text
            FROM agent_traces
            {where}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        count_query = f"SELECT COUNT(*) AS total FROM agent_traces {where}"

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)
            count_rows = await fetch_all(
                conn,
                count_query,
                {k: v for k, v in qparams.items() if k not in ("limit", "offset")},
            )

        total = _row_int(count_rows[0], "total") if count_rows else 0
        traces = [_trace_payload(row) for row in rows]
        return traces, total

    async def get_trace_details(self, trace_id: str) -> JsonDict | None:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT id::text, tenant_id, agent_id, session_id, user_id,
                       graph_name, tool_calls, token_usage, timing_ms,
                       security_flags, metadata, provenance, created_at::text
                FROM agent_traces
                WHERE id::text = %(trace_id)s
                LIMIT 1
                """,
                {"trace_id": trace_id},
            )
        if not rows:
            return None
        return _trace_payload(rows[0])

    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict:
        cols = [
            ("agent_id", "agent_ids"),
            ("tenant_id", "tenant_ids"),
            ("graph_name", "graph_names"),
            ("user_id", "user_ids"),
        ]
        qparams: dict[str, object] = {}
        if tenant_id:
            qparams["tenant_id"] = tenant_id
        tenant_clause = "AND tenant_id = %(tenant_id)s" if tenant_id else ""

        filter_options: JsonDict = {}
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            for col, key in cols:
                query = (
                    f"SELECT DISTINCT {col} FROM agent_traces "
                    f"WHERE {col} IS NOT NULL AND {col} != '' {tenant_clause} "
                    f"ORDER BY {col} LIMIT 100"
                )
                rows = await fetch_all(conn, query, qparams)
                values: list[JsonValue] = [
                    as_str(row.get(col), default="") for row in rows if row.get(col)
                ]
                filter_options[key] = values
        return filter_options

    async def get_session_traces(self, *, session_id: str, tenant_id: str | None) -> list[JsonDict]:
        conditions = ["session_id = %(session_id)s"]
        qparams: dict[str, object] = {"session_id": session_id}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        where = "WHERE " + " AND ".join(conditions)
        query = f"""
            SELECT id::text, tenant_id, agent_id, session_id, user_id,
                   graph_name, tool_calls, token_usage, timing_ms,
                   security_flags, metadata, provenance, created_at::text
            FROM agent_traces
            {where}
            ORDER BY created_at DESC
            LIMIT 200
        """
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)
        return [_trace_payload(row) for row in rows]

    async def get_trace_tenant(self, trace_id: str) -> str | None:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                "SELECT tenant_id FROM agent_traces WHERE id::text = %(trace_id)s LIMIT 1",
                {"trace_id": trace_id},
            )
        if not rows:
            return None
        tenant = str(rows[0].get("tenant_id") or "")
        return tenant or None

    async def get_related_episodes(self, trace_id: str) -> list[JsonDict]:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT e.id::text, e.tenant_id, e.user_id, e.session_id,
                       e.content, e.metadata, e.created_at::text
                FROM episodic_events e
                JOIN agent_traces t ON t.tenant_id = e.tenant_id
                WHERE t.id::text = %(trace_id)s
                  AND (
                      e.metadata->>'trace_id' = %(trace_id)s
                      OR ABS(EXTRACT(EPOCH FROM (t.created_at - e.created_at))) < 2
                  )
                ORDER BY e.created_at DESC
                """,
                {"trace_id": trace_id},
            )
        return [_episode_payload(row) for row in rows]

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
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        if user_id:
            conditions.append("user_id = %(user_id)s")
            qparams["user_id"] = user_id
        if session_id:
            conditions.append("session_id = %(session_id)s")
            qparams["session_id"] = session_id
        if hours is not None:
            conditions.append("created_at > NOW() - make_interval(hours => %(hours)s)")
            qparams["hours"] = hours
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        qparams["limit"] = limit
        qparams["offset"] = offset

        query = f"""
            SELECT id::text, tenant_id, user_id, session_id,
                   content, metadata, created_at::text
            FROM episodic_events
            {where}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        count_query = f"SELECT COUNT(*) AS total FROM episodic_events {where}"

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)
            count_rows = await fetch_all(
                conn,
                count_query,
                {k: v for k, v in qparams.items() if k not in ("limit", "offset")},
            )

        total = _row_int(count_rows[0], "total") if count_rows else 0
        events = [_episode_payload(row) for row in rows]
        return events, total

    async def get_knowledge_nodes(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]:
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        if kind:
            conditions.append("node_kind = %(kind)s")
            qparams["kind"] = kind
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        qparams["limit"] = limit

        query = f"""
            SELECT id::text, node_kind, source_type, title,
                   LEFT(content, 200) AS content_preview, tenant_id, created_at::text
            FROM knowledge_nodes
            {where}
            ORDER BY created_at DESC
            LIMIT %(limit)s
        """
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)

        return [
            {
                "id": str(row.get("id") or ""),
                "node_kind": str(row.get("node_kind") or ""),
                "source_type": str(row.get("source_type") or ""),
                "title": str(row.get("title") or ""),
                "content_preview": str(row.get("content_preview") or ""),
                "tenant_id": str(row.get("tenant_id") or ""),
                "created_at": str(row.get("created_at") or ""),
            }
            for row in rows
        ]

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict:
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        ep_query = f"SELECT COUNT(*) AS total FROM episodic_events {where}"
        kn_query = f"SELECT COUNT(*) AS total FROM knowledge_nodes {where}"

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            ep_rows = await fetch_all(conn, ep_query, qparams)
            kn_rows = await fetch_all(conn, kn_query, qparams)

        episode_count = _row_int(ep_rows[0], "total") if ep_rows else 0
        knowledge_count = _row_int(kn_rows[0], "total") if kn_rows else 0
        return {
            "episodes": {"count": episode_count},
            "knowledge_nodes": {"count": knowledge_count},
        }

    async def get_system_analytics(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        if hours is not None:
            conditions.append("created_at > NOW() - make_interval(hours => %(hours)s)")
            qparams["hours"] = hours
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        query = f"""
            SELECT
                COUNT(*) AS total_traces,
                AVG(timing_ms)::bigint AS avg_timing_ms,
                COUNT(DISTINCT tenant_id) AS unique_tenants,
                COUNT(DISTINCT session_id) AS unique_sessions,
                COUNT(DISTINCT user_id) AS unique_users,
                COALESCE(SUM((token_usage->>'input_tokens')::numeric::bigint), 0)
                    AS total_input_tokens,
                COALESCE(SUM((token_usage->>'output_tokens')::numeric::bigint), 0)
                    AS total_output_tokens
            FROM agent_traces
            {where}
        """
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)

        row = rows[0] if rows else {}
        return {
            "total_traces": _row_int(row, "total_traces"),
            "avg_timing_ms": _row_int(row, "avg_timing_ms"),
            "unique_tenants": _row_int(row, "unique_tenants"),
            "unique_sessions": _row_int(row, "unique_sessions"),
            "unique_users": _row_int(row, "unique_users"),
            "total_input_tokens": _row_int(row, "total_input_tokens"),
            "total_output_tokens": _row_int(row, "total_output_tokens"),
        }

    async def get_analytics_summary(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        base_qparams: dict[str, object] = {}
        base_clauses: list[str] = []
        if tenant_id:
            base_clauses.append("tenant_id = %(tenant_id)s")
            base_qparams["tenant_id"] = tenant_id
        if hours is not None:
            base_clauses.append("created_at > NOW() - make_interval(hours => %(hours)s)")
            base_qparams["hours"] = hours
        tcond = " AND ".join(base_clauses)

        def _and(extra: str) -> str:
            if tcond and extra:
                return f"WHERE {extra} AND {tcond}"
            if extra:
                return f"WHERE {extra}"
            if tcond:
                return f"WHERE {tcond}"
            return ""

        def _where() -> str:
            return f"WHERE {tcond}" if tcond else ""

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                f"SELECT COUNT(*) AS n FROM agent_traces {_where()}",
                base_qparams,
            )
            total_traces = _row_int(rows[0], "n") if rows else 0
            if total_traces == 0:
                return _empty_analytics_summary()

            cond_24h = "created_at > NOW() - INTERVAL '1 day'"
            cond_1h = "created_at > NOW() - INTERVAL '1 hour'"

            rows = await fetch_all(
                conn,
                f"SELECT COUNT(*) AS n FROM agent_traces {_and(cond_24h)}",
                base_qparams,
            )
            traces_24h = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                f"SELECT COUNT(*) AS n FROM agent_traces {_and(cond_1h)}",
                base_qparams,
            )
            traces_1h = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                f"SELECT AVG(timing_ms)::int AS n FROM agent_traces {_where()}",
                base_qparams,
            )
            avg_timing_ms = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY timing_ms)::int AS n "
                "FROM agent_traces WHERE timing_ms IS NOT NULL"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            p95_timing_ms = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'input_tokens')::numeric::bigint),0) AS i, "
                f"COALESCE(SUM((token_usage->>'output_tokens')::numeric::bigint),0) AS o "
                f"FROM agent_traces {_where()}",
                base_qparams,
            )
            total_input_tokens = _row_int(rows[0], "i") if rows else 0
            total_output_tokens = _row_int(rows[0], "o") if rows else 0

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'input_tokens')::numeric::bigint),0) AS i, "
                f"COALESCE(SUM((token_usage->>'output_tokens')::numeric::bigint),0) AS o "
                f"FROM agent_traces {_and(cond_24h)}",
                base_qparams,
            )
            tokens_24h_in = _row_int(rows[0], "i") if rows else 0
            tokens_24h_out = _row_int(rows[0], "o") if rows else 0

            rows = await fetch_all(
                conn,
                "SELECT COUNT(DISTINCT session_id) AS n FROM agent_traces "
                "WHERE session_id IS NOT NULL AND session_id != ''"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            unique_sessions = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                "SELECT COUNT(DISTINCT user_id) AS n FROM agent_traces "
                "WHERE user_id IS NOT NULL AND user_id != ''" + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            unique_users = _row_int(rows[0], "n") if rows else 0

            tool_where = "WHERE tool->>'tool' IS NOT NULL AND tool->>'tool' != ''"
            if tcond:
                tool_where += f" AND {tcond}"
            rows = await fetch_all(
                conn,
                f"SELECT tool->>'tool' AS tool_name, COUNT(*) AS n "
                f"FROM agent_traces, jsonb_array_elements(tool_calls) AS tool "
                f"{tool_where} "
                f"GROUP BY tool_name ORDER BY n DESC LIMIT 10",
                base_qparams,
            )
            tool_usage: JsonDict = {
                as_str(row.get("tool_name"), default=""): _row_int(row, "n")
                for row in rows
                if row.get("tool_name")
            }

            rows = await fetch_all(
                conn,
                f"SELECT date_trunc('hour', created_at) AS hour, COUNT(*) AS n "
                f"FROM agent_traces {_and(cond_24h)} "
                f"GROUP BY hour ORDER BY hour",
                base_qparams,
            )
            traces_per_hour: list[JsonValue] = []
            for row in rows:
                hour_val = row.get("hour")
                if isinstance(hour_val, datetime):
                    hour_str = hour_val.strftime("%H:%M")
                else:
                    hour_str = str(hour_val) if hour_val is not None else ""
                traces_per_hour.append({"hour": hour_str, "count": _row_int(row, "n")})

            rows = await fetch_all(
                conn,
                "SELECT COUNT(*) AS n FROM agent_traces "
                "WHERE jsonb_array_length(COALESCE(security_flags->'events','[]'::jsonb)) > 0"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            security_event_count = _row_int(rows[0], "n") if rows else 0

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'total_cost')::numeric),0) AS n "
                f"FROM agent_traces {_where()}",
                base_qparams,
            )
            total_cost = _row_float(rows[0], "n") if rows else 0.0

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'total_cost')::numeric),0) AS n "
                f"FROM agent_traces {_and(cond_24h)}",
                base_qparams,
            )
            cost_24h_total = _row_float(rows[0], "n") if rows else 0.0

        return {
            "total_traces": total_traces,
            "traces_24h": traces_24h,
            "traces_1h": traces_1h,
            "avg_timing_ms": avg_timing_ms,
            "p95_timing_ms": p95_timing_ms,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "tokens_24h_in": tokens_24h_in,
            "tokens_24h_out": tokens_24h_out,
            "tokens_24h_total": tokens_24h_in + tokens_24h_out,
            "unique_sessions": unique_sessions,
            "unique_users": unique_users,
            "tool_usage": tool_usage,
            "traces_per_hour": traces_per_hour,
            "security_event_count": security_event_count,
            "total_cost": total_cost,
            "cost_24h_total": cost_24h_total,
        }


__all__ = ["PostgresAdminOps"]
