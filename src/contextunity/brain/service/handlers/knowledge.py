"""Knowledge handlers - search, upsert, KG operations."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from ...core.config import get_core_config
from ...core.exceptions import BrainCellNotFoundError
from ...payloads import (
    CreateKGRelationPayload,
    GetCellPayload,
    GraphSearchPayload,
    QueryCellsPayload,
    QueryMemoryPayload,
    SearchPayload,
    UpsertCellPayload,
    UpsertPayload,
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

logger = get_contextunit_logger(__name__)


async def _query_vector(*, storage: BrainHandlerBase, text: str) -> list[float]:
    """Generate a vector only when the selected storage can consume it."""
    dimension = get_core_config().embeddings.dimension
    if not text or not storage.storage.vector_backend_available():
        return [0.0] * dimension
    return await storage.embedder.embed_async(text)


class KnowledgeHandlersMixin(BrainHandlerBase):
    """Mixin for core knowledge gRPC handlers."""

    @grpc_stream_error_handler
    async def Search(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Semantic/Hybrid search."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = SearchPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        query_vec = await _query_vector(storage=self, text=params.query_text)

        results = await self.storage.hybrid_search(
            query_text=params.query_text,
            query_vec=query_vec,
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            limit=params.limit,
            source_types=params.source_types if params.source_types else None,
        )

        for res in results:
            yield make_response(
                payload={
                    "id": res.node.id,
                    "content": res.node.content or "",
                    "score": res.score or 0.0,
                    "source_type": res.node.source_type or "",
                    "metadata": {k: str(v) for k, v in (res.node.metadata or {}).items()},
                },
                parent_unit=unit,  # Inherit trace_id and extend provenance
            )

    @grpc_error_handler
    async def GraphSearch(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Graph traversal search.

        Walks cell_edges from entrypoint_ids up to max_hops.
        Returns discovered nodes with attributes and edges with weights.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GraphSearchPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        result = await self.storage.graph_search(
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            entrypoint_ids=params.entrypoint_ids,
            max_hops=params.max_hops,
            allowed_relations=params.allowed_relations or None,
            max_results=params.max_results,
        )

        logger.info(
            "GraphSearch: tenant=%s entrypoints=%d hops=%d -> nodes=%d edges=%d",
            params.tenant_id,
            len(params.entrypoint_ids),
            params.max_hops,
            len(result.get("nodes", [])),
            len(result.get("edges", [])),
        )

        return make_response(
            payload={
                "nodes": result.get("nodes", []),
                "edges": result.get("edges", []),
            },
            parent_unit=unit,
        )

    @grpc_error_handler
    async def CreateKGRelation(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Create Knowledge Graph relation."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = CreateKGRelationPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        from contextunity.brain.storage.postgres.models import GraphEdge

        edge = GraphEdge(
            source_id=f"{params.source_type}:{params.source_id}",
            target_id=f"{params.target_type}:{params.target_id}",
            relation=params.relation,
            weight=1.0,
            metadata={},
        )

        await self.storage.upsert_graph(
            nodes=[],
            edges=[edge],
            tenant_id=params.tenant_id,
            user_id=params.user_id,
        )

        logger.info(
            f"Created KG relation: {params.source_id} -[{params.relation}]-> {params.target_id}"
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def Upsert(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Generic content upsert."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertPayload.model_validate(unit.payload or {})
        validate_tenant_write_policy(
            token,
            params.tenant_id,
            context,
            content=params.content,
            cell_kind="document",
            source_type=params.source_type,
        )
        validate_user_access(token, params.user_id, context)

        from contextunity.brain.ingest import IngestionService

        service = IngestionService(self.storage)
        doc_id = await service.ingest_document(
            content=params.content,
            metadata=params.metadata,
            embedder=self.embedder,
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            source_type=params.source_type,
        )

        return make_response(
            payload={"id": doc_id, "success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def UpsertCell(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Upsert canonical BrainCell using cells storage with content_hash idempotency."""
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
        """Query canonical BrainCells."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = QueryCellsPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        validate_user_access(token, params.user_id, context)

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

        for res in results:
            yield make_response(
                payload=JsonDict(
                    {
                        "id": res.get("id"),
                        "tenant_id": res.get("tenant_id"),
                        "cell_kind": res.get("cell_kind"),
                        "content": res.get("content"),
                        "metadata": res.get("metadata"),
                        "score": res.get("score"),
                        "source_type": res.get("source_type"),
                        "source_ref": res.get("source_ref"),
                        "scope_path": res.get("scope_path"),
                        "content_hash": res.get("content_hash"),
                        "confidence": res.get("confidence"),
                        "visibility": res.get("visibility"),
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
        """Get single BrainCell by ID."""
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

        return make_response(
            payload=JsonDict(
                {
                    "id": result.get("id"),
                    "tenant_id": result.get("tenant_id"),
                    "cell_kind": result.get("cell_kind"),
                    "content": result.get("content"),
                    "metadata": result.get("metadata"),
                    "source_type": result.get("source_type"),
                    "source_ref": result.get("source_ref"),
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
    async def QueryMemory(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Hybrid search for relevant knowledge (legacy, use Search)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = QueryMemoryPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        query_vec = await _query_vector(storage=self, text=params.content)

        results = await self.storage.hybrid_search(
            query_text=params.content,
            query_vec=query_vec,
            tenant_id=params.tenant_id,
            user_id=params.user_id,
        )

        for res in results:
            yield make_response(
                payload={
                    "content": res.node.content,
                    "metadata": res.node.metadata,
                    "score": res.score,
                },
                parent_unit=unit,  # Inherit trace_id and extend provenance
            )


__all__ = ["KnowledgeHandlersMixin"]
