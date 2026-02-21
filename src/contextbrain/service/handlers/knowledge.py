"""Knowledge handlers - search, upsert, KG operations."""

from __future__ import annotations

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler
from contextcore.permissions import Permissions

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
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


class KnowledgeHandlersMixin:
    """Mixin for core knowledge gRPC handlers."""

    @grpc_stream_error_handler
    async def Search(self, request, context):
        """Semantic/Hybrid search."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = SearchPayload(**unit.payload)

        query_vec = (
            await self.embedder.embed_async(params.query_text)
            if params.query_text
            else [0.1] * 1536
        )

        results = await self.storage.hybrid_search(
            query_text=params.query_text,
            query_vec=query_vec,
            tenant_id=params.tenant_id,
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
                provenance=["brain:search"],
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

        result = await self.storage.graph_search(
            tenant_id=params.tenant_id,
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
            provenance=["brain:graph_search"],
        )

    @grpc_error_handler
    async def CreateKGRelation(self, request, context):
        """Create Knowledge Graph relation."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = CreateKGRelationPayload(**unit.payload)

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
        )

        logger.info(
            f"Created KG relation: {params.source_id} -[{params.relation}]-> {params.target_id}"
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
            provenance=["brain:create_kg"],
        )

    @grpc_error_handler
    async def Upsert(self, request, context):
        """Generic content upsert."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertPayload(**unit.payload)

        from contextbrain.ingest import IngestionService

        service = IngestionService(self.storage)
        doc_id = await service.ingest_document(
            content=params.content,
            metadata=params.metadata,
            embedder=self.embedder,
            tenant_id=params.tenant_id,
            source_type=params.source_type,
        )

        return make_response(
            payload={"id": doc_id, "success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
            provenance=["brain:upsert"],
        )

    @grpc_stream_error_handler
    async def QueryMemory(self, request, context):
        """Hybrid search for relevant knowledge (legacy, use Search)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = QueryMemoryPayload(**unit.payload)

        query_vec = (
            await self.embedder.embed_async(params.content) if params.content else [0.1] * 1536
        )

        results = await self.storage.hybrid_search(
            query_text=params.content,
            query_vec=query_vec,
            tenant_id=params.tenant_id,
        )

        for res in results:
            yield make_response(
                payload={
                    "content": res.node.content,
                    "metadata": res.node.metadata,
                    "score": res.score,
                },
                parent_unit=unit,  # Inherit trace_id and extend provenance
                provenance=["brain:query_memory"],
            )


__all__ = ["KnowledgeHandlersMixin"]
