"""Agent traces storage operations."""

from __future__ import annotations

import uuid
from typing import Any, List

from .helpers import Json, execute, fetch_all


class TracesMixin:
    """Mixin for agent trace persistence and retrieval."""

    async def log_trace(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        session_id: str | None = None,
        user_id: str | None = None,
        graph_name: str | None = None,
        tool_calls: list[dict[str, Any]] | None = None,
        token_usage: dict[str, Any] | None = None,
        timing_ms: int | None = None,
        security_flags: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        provenance: list[str] | None = None,
    ) -> str:
        """Log an agent execution trace.

        Returns:
            Generated trace UUID.
        """
        trace_id = str(uuid.uuid4())
        async with await self.tenant_connection(tenant_id) as conn:
            await execute(
                conn,
                """
                INSERT INTO agent_traces
                    (id, tenant_id, agent_id, session_id, user_id, graph_name,
                     tool_calls, token_usage, timing_ms, security_flags, metadata,
                     provenance)
                VALUES
                    (%(id)s, %(tenant_id)s, %(agent_id)s, %(session_id)s, %(user_id)s,
                     %(graph_name)s, %(tool_calls)s, %(token_usage)s, %(timing_ms)s,
                     %(security_flags)s, %(metadata)s, %(provenance)s)
                """,
                {
                    "id": trace_id,
                    "tenant_id": tenant_id,
                    "agent_id": agent_id,
                    "session_id": session_id,
                    "user_id": user_id,
                    "graph_name": graph_name,
                    "tool_calls": Json(tool_calls or []),
                    "token_usage": Json(token_usage or {}),
                    "timing_ms": timing_ms,
                    "security_flags": Json(security_flags or {}),
                    "metadata": Json(metadata or {}),
                    "provenance": provenance,
                },
            )
        return trace_id

    async def get_traces(
        self,
        *,
        tenant_id: str,
        agent_id: str | None = None,
        session_id: str | None = None,
        limit: int = 20,
        since: str | None = None,
    ) -> List[dict]:
        """Get agent traces with optional filters.

        Args:
            tenant_id: Required tenant filter.
            agent_id: Optional filter by agent.
            session_id: Optional filter by session.
            limit: Max results (default 20).
            since: ISO timestamp — only return traces after this time.

        Returns:
            List of trace dicts ordered by created_at DESC.
        """
        conditions = ["tenant_id = %(tenant_id)s"]
        params: dict[str, Any] = {"tenant_id": tenant_id, "limit": limit}

        if agent_id:
            conditions.append("agent_id = %(agent_id)s")
            params["agent_id"] = agent_id

        if session_id:
            conditions.append("session_id = %(session_id)s")
            params["session_id"] = session_id

        if since:
            conditions.append("created_at > %(since)s::timestamptz")
            params["since"] = since

        where = " AND ".join(conditions)

        async with await self.tenant_connection(tenant_id) as conn:
            return await fetch_all(
                conn,
                f"""
                SELECT id, tenant_id, agent_id, session_id, user_id, graph_name,
                       tool_calls, token_usage, timing_ms, security_flags,
                       metadata, provenance, created_at
                FROM agent_traces
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT %(limit)s
                """,
                params,
            )


__all__ = ["TracesMixin"]
