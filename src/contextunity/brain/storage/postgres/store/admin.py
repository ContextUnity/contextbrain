"""PostgreSQL admin observability queries for Brain Admin RPCs."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from contextunity.core.narrowing import as_float, as_int, as_str
from contextunity.core.types import JsonDict, JsonValue

from ...embedding_jobs import embedding_job_status_counts, first_row
from .helpers import fetch_all


def _row_int(row: JsonDict, key: str) -> int:
    return as_int(row.get(key), default=0)


def _row_float(row: JsonDict, key: str) -> float:
    return as_float(row.get(key), default=0.0)


def _first_row_int(rows: list[JsonDict], key: str) -> int:
    """Read one aggregate result without exposing list-position semantics."""
    row = first_row(rows)
    return _row_int(row, key) if row is not None else 0


def _first_row_float(rows: list[JsonDict], key: str) -> float:
    """Read one aggregate float result without exposing list-position semantics."""
    row = first_row(rows)
    return _row_float(row, key) if row is not None else 0.0


if TYPE_CHECKING:
    from contextunity.brain.storage.postgres.store import PostgresBrainStore


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

    def __init__(self, storage: PostgresBrainStore) -> None:
        self._storage = storage

    async def list_tenants(self) -> list[JsonDict]:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT tenant_id, COUNT(*) AS trace_count
                FROM event_journal
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
            FROM event_journal
            {where}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        count_query = f"SELECT COUNT(*) AS total FROM event_journal {where}"

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)
            count_rows = await fetch_all(
                conn,
                count_query,
                {k: v for k, v in qparams.items() if k not in ("limit", "offset")},
            )

        total = _first_row_int(count_rows, "total")
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
                FROM event_journal
                WHERE id::text = %(trace_id)s
                LIMIT 1
                """,
                {"trace_id": trace_id},
            )
        row = first_row(rows)
        if row is None:
            return None
        return _trace_payload(row)

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
                    f"SELECT DISTINCT {col} FROM event_journal "
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
            FROM event_journal
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
                "SELECT tenant_id FROM event_journal WHERE id::text = %(trace_id)s LIMIT 1",
                {"trace_id": trace_id},
            )
        row = first_row(rows)
        if row is None:
            return None
        tenant = str(row.get("tenant_id") or "")
        return tenant or None

    async def get_related_episodes(self, trace_id: str) -> list[JsonDict]:
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT e.id::text, e.tenant_id, e.user_id, e.session_id,
                       e.content, e.metadata, e.created_at::text
                FROM episodic_events e
                JOIN event_journal t ON t.tenant_id = e.tenant_id
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

        total = _first_row_int(count_rows, "total")
        events = [_episode_payload(row) for row in rows]
        return events, total

    async def get_cells(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]:
        conditions: list[str] = []
        qparams: dict[str, object] = {}
        if tenant_id:
            conditions.append("tenant_id = %(tenant_id)s")
            qparams["tenant_id"] = tenant_id
        if kind:
            conditions.append("cell_kind = %(kind)s")
            qparams["kind"] = kind
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        qparams["limit"] = limit

        query = f"""
            SELECT id::text, cell_kind, source_type, title,
                   LEFT(content, 200) AS content_preview, tenant_id, created_at::text
            FROM cells
            {where}
            ORDER BY created_at DESC
            LIMIT %(limit)s
        """
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)

        return [
            {
                "id": str(row.get("id") or ""),
                "cell_kind": str(row.get("cell_kind") or ""),
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
        cells_query = f"SELECT COUNT(*) AS total FROM cells {where}"
        source_query = f"""
            SELECT source_type, COUNT(*) AS total
            FROM cells
            {where}
            GROUP BY source_type
            ORDER BY source_type
        """
        jobs_query = f"""
            SELECT status, COUNT(*)::integer AS count
            FROM cell_embedding_jobs
            {where}
            GROUP BY status
            ORDER BY status
        """

        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            ep_rows = await fetch_all(conn, ep_query, qparams)
            cells_rows = await fetch_all(conn, cells_query, qparams)
            source_rows = await fetch_all(conn, source_query, qparams)
            job_rows = await fetch_all(conn, jobs_query, qparams)

        episode = first_row(ep_rows)
        cells = first_row(cells_rows)
        episode_count = _row_int(episode, "total") if episode is not None else 0
        cells_count = _row_int(cells, "total") if cells is not None else 0
        source_types: JsonDict = {
            str(row.get("source_type") or "unknown"): _row_int(row, "total") for row in source_rows
        }
        return {
            "episodic_events": {"count": episode_count},
            "cells": {"count": cells_count, "by_source_type": source_types},
            "embedding_jobs": embedding_job_status_counts(job_rows),
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
            FROM event_journal
            {where}
        """
        async with await self._storage.tenant_connection("*", user_id="*") as conn:
            rows = await fetch_all(conn, query, qparams)

        row = first_row(rows) or {}
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
                f"SELECT COUNT(*) AS n FROM event_journal {_where()}",
                base_qparams,
            )
            total_traces = _first_row_int(rows, "n")
            if total_traces == 0:
                return _empty_analytics_summary()

            cond_24h = "created_at > NOW() - INTERVAL '1 day'"
            cond_1h = "created_at > NOW() - INTERVAL '1 hour'"

            rows = await fetch_all(
                conn,
                f"SELECT COUNT(*) AS n FROM event_journal {_and(cond_24h)}",
                base_qparams,
            )
            traces_24h = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                f"SELECT COUNT(*) AS n FROM event_journal {_and(cond_1h)}",
                base_qparams,
            )
            traces_1h = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                f"SELECT AVG(timing_ms)::int AS n FROM event_journal {_where()}",
                base_qparams,
            )
            avg_timing_ms = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY timing_ms)::int AS n "
                "FROM event_journal WHERE timing_ms IS NOT NULL"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            p95_timing_ms = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'input_tokens')::numeric::bigint),0) AS i, "
                f"COALESCE(SUM((token_usage->>'output_tokens')::numeric::bigint),0) AS o "
                f"FROM event_journal {_where()}",
                base_qparams,
            )
            total_input_tokens = _first_row_int(rows, "i")
            total_output_tokens = _first_row_int(rows, "o")

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'input_tokens')::numeric::bigint),0) AS i, "
                f"COALESCE(SUM((token_usage->>'output_tokens')::numeric::bigint),0) AS o "
                f"FROM event_journal {_and(cond_24h)}",
                base_qparams,
            )
            tokens_24h_in = _first_row_int(rows, "i")
            tokens_24h_out = _first_row_int(rows, "o")

            rows = await fetch_all(
                conn,
                "SELECT COUNT(DISTINCT session_id) AS n FROM event_journal "
                "WHERE session_id IS NOT NULL AND session_id != ''"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            unique_sessions = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                "SELECT COUNT(DISTINCT user_id) AS n FROM event_journal "
                "WHERE user_id IS NOT NULL AND user_id != ''" + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            unique_users = _first_row_int(rows, "n")

            tool_where = "WHERE tool->>'tool' IS NOT NULL AND tool->>'tool' != ''"
            if tcond:
                tool_where += f" AND {tcond}"
            rows = await fetch_all(
                conn,
                f"SELECT tool->>'tool' AS tool_name, COUNT(*) AS n "
                f"FROM event_journal, jsonb_array_elements(tool_calls) AS tool "
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
                f"FROM event_journal {_and(cond_24h)} "
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
                "SELECT COUNT(*) AS n FROM event_journal "
                "WHERE jsonb_array_length(COALESCE(security_flags->'events','[]'::jsonb)) > 0"
                + (f" AND {tcond}" if tcond else ""),
                base_qparams,
            )
            security_event_count = _first_row_int(rows, "n")

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'total_cost')::numeric),0) AS n "
                f"FROM event_journal {_where()}",
                base_qparams,
            )
            total_cost = _first_row_float(rows, "n")

            rows = await fetch_all(
                conn,
                f"SELECT COALESCE(SUM((token_usage->>'total_cost')::numeric),0) AS n "
                f"FROM event_journal {_and(cond_24h)}",
                base_qparams,
            )
            cost_24h_total = _first_row_float(rows, "n")

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

    # ── BrainCell canonical (Phase 3, delegated to main store) ─────

    async def upsert_cell(
        self,
        *,
        tenant_id: str,
        cell_kind: str,
        content: str,
        metadata: JsonDict | None = None,
        cell_id: str | None = None,
        user_id: str | None = None,
        scope_path: str | None = None,
        content_hash: str | None = None,
        source_type: str = "manual",
        source_ref: str | None = None,
        confidence: float = 0.5,
        visibility: str = "tenant",
    ) -> JsonDict:
        return await self._storage.upsert_cell(
            tenant_id=tenant_id,
            cell_kind=cell_kind,
            content=content,
            metadata=metadata,
            cell_id=cell_id,
            user_id=user_id,
            scope_path=scope_path,
            content_hash=content_hash,
            source_type=source_type,
            source_ref=source_ref,
            confidence=confidence,
            visibility=visibility,
        )

    async def query_cells(
        self,
        *,
        tenant_id: str,
        query_text: str | None = None,
        cell_kind: str | None = None,
        source_type: str | None = None,
        scope_path: str | None = None,
        metadata_filter: JsonDict | None = None,
        limit: int = 10,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[JsonDict]:
        return await self._storage.query_cells(
            tenant_id=tenant_id,
            query_text=query_text,
            cell_kind=cell_kind,
            source_type=source_type,
            scope_path=scope_path,
            metadata_filter=metadata_filter,
            limit=limit,
            offset=offset,
            user_id=user_id,
        )

    async def get_cell(
        self, *, tenant_id: str, cell_id: str, user_id: str | None = None
    ) -> JsonDict | None:
        return await self._storage.get_cell(tenant_id=tenant_id, cell_id=cell_id, user_id=user_id)


__all__ = ["PostgresAdminOps"]
