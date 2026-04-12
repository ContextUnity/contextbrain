"""Knowledge handlers - search, upsert, KG operations."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions

from ...payloads import (
    CreateKGRelationPayload,
    GraphSearchPayload,
    QueryMemoryPayload,
    SearchPayload,
    UpsertPayload,
)
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


class KnowledgeHandlersMixin:
    """Mixin for core knowledge gRPC handlers."""

    @grpc_stream_error_handler
    async def Search(self, request, context):
        """Semantic/Hybrid search."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = SearchPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        query_vec = (
            await self.embedder.embed_async(params.query_text)
            if params.query_text
            else [0.1] * 1536
        )

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
    async def GraphSearch(self, request, context):
        """Graph traversal search.

        Walks knowledge_edges from entrypoint_ids up to max_hops.
        Returns discovered nodes with attributes and edges with weights.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GraphSearchPayload(**unit.payload)
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
    async def CreateKGRelation(self, request, context):
        """Create Knowledge Graph relation."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = CreateKGRelationPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        from ..storage.postgres.models import GraphEdge

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
    async def Upsert(self, request, context):
        """Generic content upsert."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)
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

    @grpc_stream_error_handler
    async def QueryMemory(self, request, context):
        """Hybrid search for relevant knowledge (legacy, use Search)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = QueryMemoryPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)

        query_vec = (
            await self.embedder.embed_async(params.content) if params.content else [0.1] * 1536
        )

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
