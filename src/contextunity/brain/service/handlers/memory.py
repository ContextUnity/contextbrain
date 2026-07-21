"""Brain-owned Conversation History and retention handlers."""

from __future__ import annotations

import hmac
from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.sdk.conversation import conversation_retention_evidence_hash
from contextunity.core.tokens import ContextToken

from ...core.exceptions import BrainValidationError
from ...payloads import (
    AppendConversationRecordPayload,
    ApplyConversationRetentionPayload,
    ApplyExecutionTraceRetentionPayload,
    GetConversationHistoryStatsPayload,
    QueryConversationHistoryPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_tenant_write_policy,
    validate_token_for_read,
    validate_token_for_write,
    validate_user_access,
)
from ..read_bulkhead import get_brain_read_bulkhead

logger = get_contextunit_logger(__name__)


def _conversation_read_permission(token: ContextToken | None) -> str:
    """Select the narrowest shipped read permission for the verified caller."""
    if isinstance(token, ContextToken) and token.has_permission(Permissions.MEMORY_READ):
        return Permissions.MEMORY_READ
    return Permissions.ADMIN_READ


class MemoryHandlersMixin(BrainHandlerBase):
    """Canonical Conversation History handlers."""

    @grpc_error_handler
    async def AppendConversationRecord(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Append one immutable tenant/user-scoped conversation record."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = AppendConversationRecordPayload.model_validate(unit.payload or {})
        validate_tenant_write_policy(
            token,
            params.tenant_id,
            context,
            content=params.content,
            source_type="conversation_history",
            record_kind=params.kind,
        )
        validate_user_access(token, params.user_id, context)
        receipt = await self.storage.append_conversation_record(
            record_id=params.record_id,
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            session_id=params.session_id,
            role=params.role,
            kind=params.kind,
            content=params.content,
            content_hash=params.content_hash,
            source_hash=params.source_hash,
            graph_run_id=params.graph_run_id,
            metadata_version=params.metadata_version,
            idempotency_key=params.idempotency_key,
            metadata=params.metadata,
        )
        return make_response(
            payload={
                "record_id": str(receipt.record_id),
                "outcome": receipt.outcome,
                "content_hash": receipt.content_hash,
                "source_hash": receipt.source_hash,
            },
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def QueryConversationHistory(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Return one bounded canonical history projection."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(
            unit,
            token,
            context,
            required_permission=_conversation_read_permission(token),
        )
        params = QueryConversationHistoryPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        if not (isinstance(token, ContextToken) and token.has_permission(Permissions.ADMIN_READ)):
            validate_user_access(
                token,
                params.user_id if params.projection == "recent" else None,
                context,
            )
        async with get_brain_read_bulkhead().acquire(params.tenant_id):
            records = await self.storage.query_conversation_history(
                tenant_id=params.tenant_id,
                projection=params.projection,
                user_id=params.user_id,
                session_id=params.session_id,
                graph_run_id=params.graph_run_id,
                older_than_days=params.older_than_days,
                limit=params.limit,
                offset=params.offset,
            )
        for record in records:
            yield make_response(
                payload={
                    "record_id": str(record.record_id),
                    "tenant_id": record.tenant_id,
                    "user_id": record.user_id,
                    "session_id": record.session_id,
                    "role": record.role,
                    "kind": record.kind,
                    "content": record.content,
                    "content_hash": record.content_hash,
                    "source_hash": record.source_hash,
                    "graph_run_id": (
                        str(record.graph_run_id) if record.graph_run_id is not None else None
                    ),
                    "created_at": record.created_at.isoformat(),
                    "metadata_version": record.metadata_version,
                    "idempotency_key": record.idempotency_key,
                    "metadata": record.metadata,
                },
                parent_unit=unit,
            )

    @grpc_error_handler
    async def GetConversationHistoryStats(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Return content-free tenant statistics."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(
            unit,
            token,
            context,
            required_permission=_conversation_read_permission(token),
        )
        params = GetConversationHistoryStatsPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        stats = await self.storage.get_conversation_history_stats(tenant_id=params.tenant_id)
        return make_response(
            payload={
                "tenant_id": stats.tenant_id,
                "total": stats.total,
                "oldest": stats.oldest.isoformat() if stats.oldest is not None else None,
                "newest": stats.newest.isoformat() if stats.newest is not None else None,
            },
            parent_unit=unit,
        )

    @grpc_error_handler
    async def ApplyConversationRetention(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Apply explicit owner retention with immutable evidence fields."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        params = ApplyConversationRetentionPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, None, context)
        expected_evidence = conversation_retention_evidence_hash(
            tenant_id=params.tenant_id,
            cutoff=params.cutoff,
            record_ids=params.record_ids,
        )
        if not hmac.compare_digest(params.hold_evidence_hash, expected_evidence):
            raise BrainValidationError("conversation retention evidence is stale or mismatched")
        receipt = await self.storage.apply_conversation_retention(
            tenant_id=params.tenant_id,
            record_ids=params.record_ids,
            cutoff=params.cutoff,
            policy_version=params.policy_version,
            hold_evidence_hash=params.hold_evidence_hash,
        )
        return make_response(
            payload={
                "tenant_id": receipt.tenant_id,
                "deleted_count": receipt.deleted_count,
                "policy_version": receipt.policy_version,
                "hold_evidence_hash": receipt.hold_evidence_hash,
            },
            parent_unit=unit,
        )

    @grpc_error_handler
    async def ApplyExecutionTraceRetention(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Apply terminal Execution Trace retention without conversation selectors."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ApplyExecutionTraceRetentionPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, None, context)
        deleted = await self.storage.delete_old_execution_traces(
            tenant_id=params.tenant_id,
            older_than_days=params.older_than_days,
        )
        logger.info(
            "ApplyExecutionTraceRetention: deleted %d traces (tenant=%s, days=%d)",
            deleted,
            params.tenant_id,
            params.older_than_days,
        )
        return make_response(
            payload={"tenant_id": params.tenant_id, "deleted_count": deleted},
            parent_unit=unit,
        )


__all__ = ["MemoryHandlersMixin"]
