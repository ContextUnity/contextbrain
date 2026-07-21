"""Agent traces storage operations."""

from __future__ import annotations

import uuid
from abc import ABC
from hashlib import sha256
from json import dumps as canonical_dumps

from contextunity.core.logging import get_contextunit_logger
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError

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
                INSERT INTO execution_traces
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
        async with await self.tenant_connection(tenant_id) as conn:
            inserted = await fetch_all(
                conn,
                """
                INSERT INTO execution_traces
                    (id, tenant_id, agent_id, session_id, user_id, graph_name,
                     token_usage, timing_ms, security_flags, metadata, provenance,
                     graph_run_id, payload_digest, terminal_status, terminal_reason,
                     trace_schema_version, prompt_evidence, steps, control_evidence, final_verdict)
                VALUES
                    (%(id)s, %(tenant_id)s, %(agent_id)s, %(session_id)s, %(user_id)s,
                     %(graph_name)s, %(token_usage)s, %(timing_ms)s, %(security_flags)s,
                     %(metadata)s, %(provenance)s, %(graph_run_id)s, %(payload_digest)s,
                     %(terminal_status)s, %(terminal_reason)s, %(trace_schema_version)s,
                     %(prompt_evidence)s, %(steps)s, %(control_evidence)s, %(final_verdict)s)
                ON CONFLICT (tenant_id, graph_run_id) WHERE graph_run_id IS NOT NULL
                DO NOTHING
                RETURNING id, payload_digest
                """,
                {
                    "id": trace_id,
                    "tenant_id": tenant_id,
                    "agent_id": str(terminal_trace["agent_id"]),
                    "session_id": terminal_trace.get("session_id"),
                    "user_id": terminal_trace.get("user_id"),
                    "graph_name": str(terminal_trace["graph_name"]),
                    "token_usage": Json(terminal_trace["usage"]),
                    "timing_ms": terminal_trace["duration_ms"],
                    "security_flags": Json({"codes": terminal_trace.get("security_flags", [])}),
                    "metadata": Json(
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
                    "provenance": terminal_trace.get("provenance", []),
                    "graph_run_id": graph_run_id,
                    "payload_digest": computed_digest,
                    "terminal_status": str(terminal_trace["terminal_status"]),
                    "terminal_reason": str(terminal_trace["terminal_reason"]),
                    "trace_schema_version": str(terminal_trace["schema_version"]),
                    "prompt_evidence": Json(terminal_trace.get("prompt_evidence", [])),
                    "steps": Json(terminal_trace.get("steps", [])),
                    "control_evidence": Json(terminal_trace.get("control_evidence", {})),
                    "final_verdict": Json(terminal_trace.get("final_verdict", {})),
                },
            )
            if inserted:
                return {
                    "trace_id": str(inserted[0].get("id", "")),
                    "graph_run_id": graph_run_id,
                    "digest": computed_digest,
                    "outcome": "created",
                }
            rows = await fetch_all(
                conn,
                """
                SELECT id, payload_digest FROM execution_traces
                WHERE tenant_id = %(tenant_id)s AND graph_run_id = %(graph_run_id)s
                """,
                {"tenant_id": tenant_id, "graph_run_id": graph_run_id},
            )
            if not rows:
                raise BrainValidationError("terminal trace finalization was not durable")
            stored_id = str(rows[0].get("id", ""))
            stored_digest = str(rows[0].get("payload_digest", ""))
            if stored_digest != computed_digest:
                raise BrainValidationError("conflicting terminal trace finalization")
            return {
                "trace_id": stored_id,
                "graph_run_id": graph_run_id,
                "digest": computed_digest,
                "outcome": "duplicate",
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
                "       metadata, provenance, created_at, graph_run_id, payload_digest,",
                "       terminal_status, terminal_reason, trace_schema_version,",
                "       prompt_evidence, steps, control_evidence, final_verdict",
                "FROM execution_traces",
                "WHERE " + where,
                "ORDER BY created_at DESC",
                "LIMIT %(limit)s",
            ]
            return await fetch_all(
                conn,
                "\n".join(query),
                params,
            )

    async def delete_old_execution_traces(
        self, *, tenant_id: str, older_than_days: int = 30
    ) -> int:
        """Delete only this tenant's traces older than the age threshold."""
        async with await self.tenant_connection(tenant_id, user_id="*") as conn:
            cursor = await conn.execute(
                """
                DELETE FROM execution_traces
                WHERE tenant_id = %(tenant_id)s
                  AND created_at < now() - make_interval(days => %(days)s)
                """,
                {"tenant_id": tenant_id, "days": older_than_days},
            )
            return cursor.rowcount or 0


__all__ = ["TracesMixin"]
