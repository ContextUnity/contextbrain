"""Memory handlers - episodes and facts."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import is_json_dict

from ...payloads import (
    AddEpisodePayload,
    GetEpisodeStatsPayload,
    GetRecentEpisodesPayload,
    GetUserFactsPayload,
    RetentionCleanupPayload,
    UpsertFactPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
    validate_user_access,
)

logger = get_contextunit_logger(__name__)


def _row_float(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


class MemoryHandlersMixin(BrainHandlerBase):
    """Mixin for episodic and entity memory handlers."""

    @grpc_error_handler
    async def AddEpisode(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Persist a conversation turn into Episodic memory."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = AddEpisodePayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        # Ensure trace_id is stored in metadata for traceability
        metadata = params.metadata.copy() if params.metadata else {}
        if unit.trace_id:
            metadata["trace_id"] = str(unit.trace_id)

        await self.storage.add_episode(
            id=str(unit.unit_id),
            user_id=params.user_id or "*",
            tenant_id=params.tenant_id,
            session_id=params.session_id,
            content=params.content,
            metadata=metadata,
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def GetRecentEpisodes(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Get recent episodes for a user."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        params = GetRecentEpisodesPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        rows = await self.storage.get_recent_episodes(
            user_id=params.user_id,
            tenant_id=params.tenant_id,
            limit=params.limit,
        )

        for row in rows:
            yield make_response(
                payload={
                    "id": str(row.get("id", "")),
                    "content": row.get("content", ""),
                    "metadata": (
                        {k: str(v) for k, v in meta.items()}
                        if is_json_dict(meta := row.get("metadata"))
                        else {}
                    ),
                    "created_at": str(row.get("created_at", "")),
                },
                parent_unit=unit,
            )

    @grpc_error_handler
    async def UpsertFact(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Update Entity memory with persistent user facts."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = UpsertFactPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        await self.storage.upsert_fact(
            user_id=params.user_id,
            tenant_id=params.tenant_id,
            key=params.key,
            value=params.value,
            confidence=params.confidence,
            source_id=params.source_id,
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def GetUserFacts(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Get all known facts about a user."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        params = GetUserFactsPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        rows = await self.storage.get_user_facts(
            user_id=params.user_id,
            tenant_id=params.tenant_id,
        )

        for row in rows:
            yield make_response(
                payload={
                    "fact_key": row.get("fact_key", ""),
                    "fact_value": row.get("fact_value", ""),
                    "confidence": _row_float(row.get("confidence"), default=1.0),
                    "updated_at": str(row.get("updated_at", "")),
                },
                parent_unit=unit,
            )

    @grpc_error_handler
    async def RetentionCleanup(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Delete old episodic events (for retention policy).

        Requires MEMORY_WRITE permission.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = RetentionCleanupPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, None, context)  # Must be an admin/system token

        deleted = await self.storage.delete_old_episodes(
            tenant_id=params.tenant_id,
            older_than_days=params.older_than_days,
            episode_ids=params.episode_ids,
        )
        logger.info(
            "RetentionCleanup: deleted %d episodes (tenant=%s, days=%d)",
            deleted,
            params.tenant_id,
            params.older_than_days,
        )
        return make_response(
            payload={"deleted_count": deleted, "tenant_id": params.tenant_id},
            parent_unit=unit,
        )

    @grpc_error_handler
    async def GetEpisodeStats(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Get episode count and date range for a tenant."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        params = GetEpisodeStatsPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)

        stats = await self.storage.count_episodes(tenant_id=params.tenant_id)
        return make_response(
            payload={
                "total": stats.get("total", 0),
                "oldest": str(stats.get("oldest", "")),
                "newest": str(stats.get("newest", "")),
                "tenant_id": params.tenant_id,
            },
            parent_unit=unit,
        )


__all__ = ["MemoryHandlersMixin"]
