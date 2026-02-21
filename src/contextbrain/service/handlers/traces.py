"""Trace handlers - agent execution traces."""

from __future__ import annotations

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler
from contextcore.permissions import Permissions

from ...payloads import GetTracesPayload, LogTracePayload
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


class TraceHandlersMixin:
    """Mixin for agent trace handlers."""

    @grpc_error_handler
    async def LogTrace(self, request, context):
        """Log an agent execution trace."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = LogTracePayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        # Use payload user_id; override only if the gRPC token carries a
        # non-empty user_id (service-to-service tokens have user_id="" which
        # must NOT overwrite the real user_id from the payload).
        user_id = params.user_id
        if token is not None:
            token_uid = getattr(token, "user_id", None)
            if token_uid:
                user_id = token_uid

        # Build complete provenance chain:
        # payload provenance (from the graph/agent) + brain storage label
        provenance = list(params.provenance or [])
        provenance.append("brain:log_trace")

        trace_id = await self.storage.log_trace(
            tenant_id=params.tenant_id,
            agent_id=params.agent_id,
            session_id=params.session_id,
            user_id=user_id,
            graph_name=params.graph_name,
            tool_calls=params.tool_calls,
            token_usage=params.token_usage,
            timing_ms=params.timing_ms,
            security_flags=params.security_flags,
            metadata=params.metadata,
            provenance=provenance,
        )

        logger.info(
            "Trace logged: %s agent=%s graph=%s provenance=%s",
            trace_id,
            params.agent_id,
            params.graph_name,
            provenance,
        )

        return make_response(
            payload={"id": trace_id, "success": True},
            parent_unit=unit,
            provenance=provenance,
        )

    @grpc_stream_error_handler
    async def GetTraces(self, request, context):
        """Get agent traces with optional filters."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.TRACE_READ)
        params = GetTracesPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        rows = await self.storage.get_traces(
            tenant_id=params.tenant_id,
            agent_id=params.agent_id,
            session_id=params.session_id,
            limit=params.limit,
            since=params.since,
        )

        for row in rows:
            yield make_response(
                payload={
                    "id": str(row.get("id", "")),
                    "agent_id": row.get("agent_id", ""),
                    "session_id": row.get("session_id", ""),
                    "user_id": row.get("user_id", ""),
                    "graph_name": row.get("graph_name", ""),
                    "tool_calls": row.get("tool_calls", []),
                    "token_usage": row.get("token_usage", {}),
                    "timing_ms": row.get("timing_ms"),
                    "security_flags": row.get("security_flags", {}),
                    "metadata": row.get("metadata", {}),
                    "provenance": row.get("provenance", []),
                    "created_at": str(row.get("created_at", "")),
                },
                parent_unit=unit,
                provenance=["brain:get_traces"],
            )


__all__ = ["TraceHandlersMixin"]
