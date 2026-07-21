"""Canonical semantic and hybrid BrainCell search handler."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2
from contextunity.core.grpc_errors import grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from ...core.config import get_core_config
from ...payloads import SearchCellsPayload
from ...storage.postgres.models import ScopePath
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_tenant_access,
    validate_token_for_read,
    validate_user_access,
)
from ..read_bulkhead import get_brain_read_bulkhead


async def _query_vector(*, service: BrainHandlerBase, text: str) -> list[float]:
    """Generate a vector only when the selected storage can consume it."""
    dimension = get_core_config().embeddings.dimension
    if not text or not service.storage.vector_backend_available():
        return [0.0] * dimension
    return await service.embedder.embed_query_async(text)


class CellSearchHandlersMixin(BrainHandlerBase):
    """Canonical ranked BrainCell retrieval RPC."""

    @grpc_stream_error_handler
    async def SearchCells(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Run tenant-scoped semantic/hybrid BrainCell retrieval."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = SearchCellsPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        validate_user_access(token, params.user_id, context)

        async with get_brain_read_bulkhead().acquire(tenant_id):
            results = await self.storage.hybrid_search(
                query_text=params.query_text,
                query_vec=await _query_vector(service=self, text=params.query_text),
                tenant_id=tenant_id,
                user_id=params.user_id,
                limit=params.limit,
                scope=ScopePath(path=params.scope_path) if params.scope_path is not None else None,
                source_types=params.source_types if params.source_types else None,
                metadata_filter=params.metadata_filter,
            )
        for result in results:
            if result.score < params.min_score:
                continue
            node = result.node
            yield make_response(
                payload=JsonDict(
                    {
                        "id": node.id,
                        "tenant_id": node.tenant_id or tenant_id,
                        "cell_kind": node.cell_kind,
                        "content": node.content or "",
                        "score": result.score,
                        "vector_score": result.vector_score,
                        "text_score": result.text_score,
                        "source_type": node.source_type or "",
                        "source_ref": node.source_ref,
                        "scope_path": node.scope_path,
                        "content_hash": node.content_hash,
                        "confidence": node.confidence,
                        "visibility": node.visibility,
                        "metadata": node.metadata,
                    }
                ),
                parent_unit=unit,
            )


__all__ = ["CellSearchHandlersMixin"]
