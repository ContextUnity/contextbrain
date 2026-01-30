"""Memory handlers - episodes and facts."""

from __future__ import annotations

from contextcore import get_context_unit_logger

from ...core.exceptions import grpc_error_handler
from ...payloads import AddEpisodePayload, UpsertFactPayload
from ..helpers import make_response, parse_unit

logger = get_context_unit_logger(__name__)


class MemoryHandlersMixin:
    """Mixin for episodic and entity memory handlers."""

    @grpc_error_handler
    async def AddEpisode(self, request, context):
        """Persist a conversation turn into Episodic memory."""
        unit = parse_unit(request)
        params = AddEpisodePayload(**unit.payload)

        await self.storage.add_episode(
            id=str(unit.unit_id),
            user_id=params.user_id,
            tenant_id=params.tenant_id,
            session_id=params.session_id,
            content=params.content,
            metadata=params.metadata,
        )
        return make_response(
            payload={"success": True},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:add_episode"],
        )

    @grpc_error_handler
    async def UpsertFact(self, request, context):
        """Update Entity memory with persistent user facts."""
        unit = parse_unit(request)
        params = UpsertFactPayload(**unit.payload)

        await self.storage.upsert_fact(
            user_id=params.user_id,
            key=params.key,
            value=params.value,
            confidence=params.confidence,
            source_id=params.source_id,
        )
        return make_response(
            payload={"success": True},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:upsert_fact"],
        )


__all__ = ["MemoryHandlersMixin"]
