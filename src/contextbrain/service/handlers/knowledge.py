"""Knowledge handlers - search, upsert, KG operations."""

from __future__ import annotations

from contextcore import get_context_unit_logger

from ...core.exceptions import grpc_error_handler, grpc_stream_error_handler
from ...payloads import (
    CreateKGRelationPayload,
    QueryMemoryPayload,
    SearchPayload,
    UpsertPayload,
)
from ..helpers import make_response, parse_unit

logger = get_context_unit_logger(__name__)


class KnowledgeHandlersMixin:
    """Mixin for core knowledge gRPC handlers."""

    @grpc_stream_error_handler
    async def Search(self, request, context):
        """Semantic/Hybrid search."""
        unit = parse_unit(request)
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
                trace_id=str(unit.trace_id),
                provenance=list(unit.provenance) + ["brain:search"],
            )

    @grpc_error_handler
    async def GraphSearch(self, request, context):
        """Graph traversal search."""
        unit = parse_unit(request)
        return make_response(
            payload={"nodes": [], "edges": []},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:graph_search"],
        )

    @grpc_error_handler
    async def CreateKGRelation(self, request, context):
        """Create Knowledge Graph relation."""
        unit = parse_unit(request)
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
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:create_kg"],
        )

    @grpc_error_handler
    async def Upsert(self, request, context):
        """Generic content upsert."""
        unit = parse_unit(request)
        params = UpsertPayload(**unit.payload)

        from ..ingest import IngestionService

        service = IngestionService(self.storage)
        doc_id = await service.ingest_document(
            content=params.content,
            metadata=params.metadata,
        )

        return make_response(
            payload={"id": doc_id, "success": True},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:upsert"],
        )

    @grpc_stream_error_handler
    async def QueryMemory(self, request, context):
        """Hybrid search for relevant knowledge (legacy, use Search)."""
        unit = parse_unit(request)
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
                trace_id=str(unit.trace_id),
                provenance=list(unit.provenance) + ["brain:query_memory"],
            )


__all__ = ["KnowledgeHandlersMixin"]
