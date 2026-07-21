"""Canonical BrainCell write, document-ingestion, and direct query handlers."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from ...core.exceptions import BrainCellNotFoundError
from ...payloads import (
    DeleteDocumentationCellsPayload,
    GetCellPayload,
    IngestDocumentPayload,
    QueryCellsPayload,
    UpsertCellPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_tenant_access,
    validate_tenant_write_policy,
    validate_token_for_read,
    validate_token_for_write,
    validate_user_access,
)
from ..read_bulkhead import get_brain_read_bulkhead


class CellWriteHandlersMixin(BrainHandlerBase):
    """Canonical BrainCell writes, document ingestion, and direct reads."""

    @grpc_error_handler
    async def IngestDocument(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Run the explicit document enrichment and ingestion pipeline."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = IngestDocumentPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_write_policy(
            token,
            tenant_id,
            context,
            content=params.content,
            cell_kind="document",
            source_type=params.source_type,
        )
        validate_user_access(token, params.user_id, context)
        from contextunity.brain.ingest import IngestionService

        document_id = await IngestionService(self.storage).ingest_document(
            content=params.content,
            metadata=params.metadata,
            embedder=self.embedder,
            tenant_id=tenant_id,
            user_id=params.user_id,
            source_type=params.source_type,
        )
        return make_response(payload={"id": document_id, "success": True}, parent_unit=unit)

    @grpc_error_handler
    async def UpsertCell(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Upsert one canonical BrainCell with content-hash idempotency."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertCellPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_write_policy(
            token,
            tenant_id,
            context,
            content=params.content,
            cell_kind=params.cell_kind,
            source_type=params.source_type,
        )
        validate_user_access(token, params.user_id, context)
        result = await self.storage.upsert_cell(
            tenant_id=tenant_id,
            user_id=params.user_id,
            cell_kind=params.cell_kind,
            content=params.content,
            metadata=params.metadata,
            cell_id=params.cell_id,
            scope_path=params.scope_path,
            content_hash=params.content_hash,
            source_type=params.source_type,
            source_ref=params.source_ref,
            confidence=params.confidence,
            visibility=params.visibility,
        )
        return make_response(
            payload=JsonDict(
                {
                    "id": result.get("id"),
                    "tenant_id": result.get("tenant_id"),
                    "cell_kind": result.get("cell_kind"),
                    "source_type": result.get("source_type"),
                    "scope_path": result.get("scope_path"),
                    "content_hash": result.get("content_hash"),
                    "confidence": result.get("confidence"),
                    "visibility": result.get("visibility"),
                    "created_at": result.get("created_at"),
                    "updated_at": result.get("updated_at"),
                }
            ),
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def QueryCells(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Query canonical BrainCells through bounded direct filters."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = QueryCellsPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        validate_user_access(token, params.user_id, context)
        async with get_brain_read_bulkhead().acquire(tenant_id):
            results = await self.storage.query_cells(
                tenant_id=tenant_id,
                query_text=params.query_text,
                cell_kind=params.cell_kind,
                source_type=params.source_type,
                scope_path=params.scope_path,
                metadata_filter=params.metadata_filter,
                limit=params.limit,
                offset=params.offset,
                user_id=params.user_id,
            )
        for result in results:
            yield make_response(
                payload=JsonDict(
                    {
                        "id": result.get("id"),
                        "tenant_id": result.get("tenant_id"),
                        "cell_kind": result.get("cell_kind"),
                        "content": result.get("content"),
                        "metadata": result.get("metadata"),
                        "score": result.get("score"),
                        "source_type": result.get("source_type"),
                        "source_ref": result.get("source_ref"),
                        "scope_path": result.get("scope_path"),
                        "content_hash": result.get("content_hash"),
                        "confidence": result.get("confidence"),
                        "visibility": result.get("visibility"),
                    }
                ),
                parent_unit=unit,
            )

    @grpc_error_handler
    async def GetCell(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Get one canonical BrainCell by id."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GetCellPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        validate_user_access(token, params.user_id, context)
        result = await self.storage.get_cell(
            tenant_id=tenant_id,
            cell_id=params.cell_id,
            user_id=params.user_id,
        )
        if result is None:
            raise BrainCellNotFoundError(
                message="BrainCell not found",
                tenant_id=tenant_id,
                cell_id=params.cell_id,
            )
        return make_response(payload=JsonDict(result), parent_unit=unit)

    @grpc_error_handler
    async def DeleteDocumentationCells(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Delete exact `_doc` cells only when every observed hash still matches."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = DeleteDocumentationCellsPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_write_policy(
            token,
            tenant_id,
            context,
            cell_kind="documentation",
            source_type="documentation",
        )
        result = await self.storage.delete_documentation_cells(
            tenant_id=tenant_id,
            targets=[(target.cell_id, target.content_hash) for target in params.targets],
        )
        return make_response(
            payload=JsonDict(
                {
                    "status": result.get("status"),
                    "deleted_count": result.get("deleted_count"),
                    "expected_count": result.get("expected_count"),
                }
            ),
            parent_unit=unit,
        )


__all__ = ["CellWriteHandlersMixin"]
