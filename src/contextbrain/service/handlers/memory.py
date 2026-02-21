"""Memory handlers - episodes and facts."""

from __future__ import annotations

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler
from contextcore.permissions import Permissions

from ...payloads import (
    AddEpisodePayload,
    GetRecentEpisodesPayload,
    GetUserFactsPayload,
    RetentionCleanupPayload,
    UpsertFactPayload,
)
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


class MemoryHandlersMixin:
    """Mixin for episodic and entity memory handlers."""

    @grpc_error_handler
    async def AddEpisode(self, request, context):
        """Persist a conversation turn into Episodic memory."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = AddEpisodePayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        # Ensure trace_id is stored in metadata for traceability
        metadata = params.metadata.copy() if params.metadata else {}
        if unit.trace_id:
            metadata["trace_id"] = str(unit.trace_id)
        if unit.provenance:
            metadata["provenance"] = list(unit.provenance)

        await self.storage.add_episode(
            id=str(unit.unit_id),
            user_id=params.user_id,
            tenant_id=params.tenant_id,
            session_id=params.session_id,
            content=params.content,
            metadata=metadata,
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,
            provenance=["brain:add_episode"],
        )

    @grpc_stream_error_handler
    async def GetRecentEpisodes(self, request, context):
        """Get recent episodes for a user."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        params = GetRecentEpisodesPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

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
                    "metadata": {k: str(v) for k, v in (row.get("metadata") or {}).items()},
                    "created_at": str(row.get("created_at", "")),
                },
                parent_unit=unit,
                provenance=["brain:get_recent_episodes"],
            )

    @grpc_error_handler
    async def UpsertFact(self, request, context):
        """Update Entity memory with persistent user facts."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = UpsertFactPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

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
            provenance=["brain:upsert_fact"],
        )

    @grpc_stream_error_handler
    async def GetUserFacts(self, request, context):
        """Get all known facts about a user."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        params = GetUserFactsPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        rows = await self.storage.get_user_facts(
            user_id=params.user_id,
            tenant_id=params.tenant_id,
        )

        for row in rows:
            yield make_response(
                payload={
                    "fact_key": row.get("fact_key", ""),
                    "fact_value": row.get("fact_value", ""),
                    "confidence": float(row.get("confidence", 1.0)),
                    "updated_at": str(row.get("updated_at", "")),
                },
                parent_unit=unit,
                provenance=["brain:get_user_facts"],
            )

    @grpc_error_handler
    async def RetentionCleanup(self, request, context):
        """Delete old episodic events (for retention policy).

        Requires MEMORY_WRITE permission.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = RetentionCleanupPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

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
            provenance=["brain:retention_cleanup"],
        )

    @grpc_error_handler
    async def GetEpisodeStats(self, request, context):
        """Get episode count and date range for a tenant."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        tenant_id = unit.payload.get("tenant_id", "default")
        validate_tenant_access(token, tenant_id, context)

        stats = await self.storage.count_episodes(tenant_id=tenant_id)
        return make_response(
            payload={
                "total": stats.get("total", 0),
                "oldest": str(stats.get("oldest", "")),
                "newest": str(stats.get("newest", "")),
                "tenant_id": tenant_id,
            },
            parent_unit=unit,
            provenance=["brain:get_episode_stats"],
        )


__all__ = ["MemoryHandlersMixin"]
