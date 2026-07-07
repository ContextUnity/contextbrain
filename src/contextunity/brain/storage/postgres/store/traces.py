"""Agent traces storage operations."""

from __future__ import annotations

import uuid
from abc import ABC

from contextunity.core.logging import get_contextunit_logger
from contextunity.core.types import JsonDict

from .base import PostgresStoreBase
from .helpers import Json, execute, fetch_all

logger = get_contextunit_logger(__name__)


class TracesMixin(PostgresStoreBase, ABC):
    """Mixin for agent trace persistence and retrieval."""

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
        """Log an agent execution trace.

        Returns:
            Generated trace UUID.
        """
        trace_id = str(uuid.uuid4())

        async with await self.tenant_connection(tenant_id, user_id=user_id) as conn:
            _ = await execute(
                conn,
                """
                INSERT INTO event_journal
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
        user_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        limit: int = 20,
        since: str | None = None,
    ) -> list[JsonDict]:
        """Get agent traces with optional filters.

        Args:
            tenant_id: Required tenant filter.
            user_id: Target user_id for RLS isolation.
            agent_id: Optional filter by agent.
            session_id: Optional filter by session.
            limit: Max results (default 20).
            since: ISO timestamp — only return traces after this time.

        Returns:
            List of trace dicts ordered by created_at DESC.
        """
        conditions = ["tenant_id = %(tenant_id)s"]
        params: dict[str, object] = {"tenant_id": tenant_id, "limit": limit}

        if user_id:
            conditions.append("user_id = %(user_id)s")
            params["user_id"] = user_id

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

        async with await self.tenant_connection(tenant_id, user_id=user_id) as conn:
            query = [
                "SELECT id, tenant_id, agent_id, session_id, user_id, graph_name,",
                "       tool_calls, token_usage, timing_ms, security_flags,",
                "       metadata, provenance, created_at",
                "FROM event_journal",
                "WHERE " + where,
                "ORDER BY created_at DESC",
                "LIMIT %(limit)s",
            ]
            return await fetch_all(
                conn,
                "\n".join(query),
                params,
            )


__all__ = ["TracesMixin"]
