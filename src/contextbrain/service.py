import os
from typing import Optional

import grpc
from contextcore import (
    brain_pb2,
    brain_pb2_grpc,
    get_context_unit_logger,
    load_shared_config_from_env,
    setup_logging,
)

from .storage.duckdb_store import DuckDBStore
from .storage.postgres.store import PostgresKnowledgeStore

logger = get_context_unit_logger(__name__)


class LocalEmbedder:
    """Local embeddings using SentenceTransformers.

    Falls back to dummy vector if SentenceTransformers is not installed,
    but logs a CRITICAL warning since semantic search will NOT work correctly.
    """

    _instance: Optional["LocalEmbedder"] = None
    _warned: bool = False

    def __init__(self, model_name: str = "all-mpnet-base-v2"):
        self._model = None
        self._model_name = model_name
        self._dim = 768  # Default for all-mpnet-base-v2
        self._fallback_mode = False

    @classmethod
    def get_instance(cls) -> "LocalEmbedder":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_model(self):
        if self._model is not None:
            return self._model
        if self._fallback_mode:
            return None
        try:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self._model_name)
            logger.info(f"Loaded embedding model: {self._model_name}")
        except ImportError:
            self._fallback_mode = True
            # CRITICAL WARNING - semantic search will NOT work!
            logger.critical(
                "=" * 60 + "\n"
                "⚠️  CRITICAL ERROR: SentenceTransformers not installed!\n"
                "⚠️  Semantic search will NOT work correctly.\n"
                "⚠️  All queries will return the same results.\n"
                "\n"
                "Install the dependency:\n"
                "    pip install sentence-transformers\n"
                "\n"
                "Or add to pyproject.toml:\n"
                '    sentence-transformers = "^3.0.0"\n'
                "=" * 60
            )
            self._model = None
        return self._model

    def embed(self, text: str) -> list[float]:
        """Generate embedding for text."""
        model = self._ensure_model()
        if model is None:
            self._warn_fallback()
            return [0.1] * self._dim

        import asyncio

        loop = asyncio.get_event_loop()
        vec = loop.run_until_complete(loop.run_in_executor(None, lambda: model.encode([text])[0]))
        return [float(x) for x in vec.tolist()]

    async def embed_async(self, text: str) -> list[float]:
        """Generate embedding for text (async)."""
        model = self._ensure_model()
        if model is None:
            self._warn_fallback()
            return [0.1] * self._dim

        import asyncio

        loop = asyncio.get_event_loop()
        vec = await loop.run_in_executor(None, lambda: model.encode([text])[0])
        return [float(x) for x in vec.tolist()]

    def _warn_fallback(self):
        """Log warning on each fallback usage (max once per 100 calls)."""
        if not LocalEmbedder._warned:
            logger.warning(
                "Using DUMMY embeddings - semantic search results are MEANINGLESS. "
                "Install sentence-transformers to fix this."
            )
            LocalEmbedder._warned = True


class BrainService(brain_pb2_grpc.BrainServiceServicer):
    """
    Unified implementation of the Brain gRPC service.
    """

    def __init__(self):
        dsn = (
            os.getenv("BRAIN_DATABASE_URL")
            or os.getenv("DATABASE_URL")
            or "postgresql://user:pass@localhost:5432/brain"
        )
        self.storage = PostgresKnowledgeStore(dsn=dsn)
        self.duckdb = DuckDBStore()  # Analytical layer
        self.embedder = LocalEmbedder.get_instance()

    async def QueryMemory(self, request, context):
        """Hybrid search (Vector + Text) for Relevant Knowledge."""
        query_text = request.payload.get("content", "")
        # Use real embeddings
        query_vec = await self.embedder.embed_async(query_text) if query_text else [0.1] * 768

        tenant_id = request.payload.get("tenant_id", "default")

        results = await self.storage.hybrid_search(
            query_text=query_text, query_vec=query_vec, tenant_id=tenant_id
        )

        for res in results:
            yield brain_pb2.ContextUnit(
                unit_id=res.node.id,
                payload={
                    "content": res.node.content,
                    "metadata": res.node.metadata,
                    "score": res.score,
                },
                modality=0,
            )

    async def Memorize(self, request, context):
        """Primary knowledge ingestion point."""
        from .ingest import IngestionService

        service = IngestionService(self.storage)
        doc_id = await service.ingest_document(
            content=request.payload.get("content", ""),
            metadata=dict(request.payload.get("metadata", {})),
        )

        request.unit_id = doc_id
        return request

    async def AddEpisode(self, request, context):
        """Persist a conversation turn into Episodic memory."""
        payload = request.payload
        await self.storage.add_episode(
            id=request.unit_id or str(os.urandom(16).hex()),
            user_id=payload.get("user_id", "anonymous"),
            tenant_id=payload.get("tenant_id", "default"),
            session_id=payload.get("session_id"),
            content=payload.get("content", ""),
            metadata=dict(payload.get("metadata", {})),
        )
        return request

    async def UpsertFact(self, request, context):
        """Update Entity memory with persistent user facts."""
        payload = request.payload
        await self.storage.upsert_fact(
            user_id=payload.get("user_id", "anonymous"),
            key=payload.get("key", "unknown"),
            value=payload.get("value"),
            confidence=payload.get("confidence", 1.0),
            source_id=payload.get("source_id"),
        )
        return request

    async def UpsertTaxonomy(self, request, context):
        """Sync YAML-to-DB or UI-to-DB taxonomy entries."""
        # Convert Protobuf Struct to proper Python dict (recursive)
        from google.protobuf.json_format import MessageToDict

        payload = MessageToDict(request.payload, preserving_proto_field_name=True)

        await self.storage.upsert_taxonomy(
            tenant_id=payload.get("tenant_id", "default"),
            domain=payload.get("domain", "general"),
            name=payload.get("name"),
            path=payload.get("path"),
            keywords=list(payload.get("keywords", [])),
            metadata=dict(payload.get("metadata", {})),
        )
        return request

    async def GetTaxonomy(self, request, context):
        """Export taxonomy from DB back to logic/file layer."""
        domain = request.payload.get("domain")
        tenant_id = request.payload.get("tenant_id", "default")

        # We need a method in storage for this
        if hasattr(self.storage, "get_all_taxonomy"):
            taxonomies = await self.storage.get_all_taxonomy(tenant_id=tenant_id, domain=domain)
            for tax in taxonomies:
                yield brain_pb2.ContextUnit(
                    payload={
                        "domain": tax["domain"],
                        "name": tax["name"],
                        "path": tax["path"],
                        "keywords": list(tax["keywords"]),
                        "metadata": dict(tax["metadata"]),
                    },
                    modality=0,
                )

    async def GetPendingVerifications(self, request, context):
        """Stream items for manual review (Gardener)."""
        # Simulated stream from temporary storage/queue
        yield brain_pb2.PendingItem(
            id="",
            content="",
            context_json="{}",
        )

    # =========================================================================
    # Commerce / Gardener Methods
    # =========================================================================

    async def Search(self, request, context):
        """Semantic/Hybrid search implementation."""
        # Use real embeddings
        query_vec = (
            await self.embedder.embed_async(request.query_text)
            if request.query_text
            else [0.1] * 768
        )

        results = await self.storage.hybrid_search(
            query_text=request.query_text,
            query_vec=query_vec,
            tenant_id=request.tenant_id,
            limit=request.limit or 10,
            source_types=list(request.source_types) if request.source_types else None,
        )

        search_results = []
        for res in results:
            search_results.append(
                brain_pb2.SearchResult(
                    id=res.node.id,
                    content=res.node.content or "",
                    score=res.score or 0.0,
                    source_type=res.node.source_type or "",
                    metadata={k: str(v) for k, v in (res.node.metadata or {}).items()},
                )
            )

        return brain_pb2.SearchResponse(results=search_results)

    async def GraphSearch(self, request, context):
        """Graph traversal search."""
        # TODO: Implement graph traversal in storage
        return brain_pb2.GraphSearchResponse(nodes=[], edges=[])

    async def GetProducts(self, request, context):
        """Get products for enrichment by IDs."""
        from google.protobuf.struct_pb2 import Struct

        product_ids = list(request.product_ids)
        tenant_id = request.tenant_id

        # Query products from storage
        # For now, using a direct query - should be abstracted to storage layer
        products = []
        if hasattr(self.storage, "get_products_by_ids"):
            raw_products = await self.storage.get_products_by_ids(
                tenant_id=tenant_id,
                product_ids=product_ids,
            )
            for p in raw_products:
                params = Struct()
                params.update(p.get("params", {}))
                enrichment = Struct()
                enrichment.update(p.get("enrichment", {}))

                products.append(
                    brain_pb2.DealerProduct(
                        id=p.get("id", 0),
                        name=p.get("name", ""),
                        category=p.get("category", ""),
                        description=p.get("description", ""),
                        brand_name=p.get("brand_name", ""),
                        params=params,
                        enrichment=enrichment,
                    )
                )

        return brain_pb2.GetProductsResponse(products=products)

    async def UpdateEnrichment(self, request, context):
        """Update product enrichment data."""
        from google.protobuf.json_format import MessageToDict

        enrichment_dict = MessageToDict(request.enrichment, preserving_proto_field_name=True)

        # Update in storage
        if hasattr(self.storage, "update_product_enrichment"):
            await self.storage.update_product_enrichment(
                tenant_id=request.tenant_id,
                product_id=request.product_id,
                enrichment=enrichment_dict,
                trace_id=request.trace_id,
                status=request.status,
            )
            logger.info(f"Updated enrichment for product {request.product_id}")
            return brain_pb2.UpdateEnrichmentResponse(success=True)

        logger.warning("Storage does not support update_product_enrichment")
        return brain_pb2.UpdateEnrichmentResponse(success=False)

    async def CreateKGRelation(self, request, context):
        """Create Knowledge Graph relation."""
        from .storage.postgres.models import GraphEdge

        edge = GraphEdge(
            source_id=f"{request.source_type}:{request.source_id}",
            target_id=f"{request.target_type}:{request.target_id}",
            relation=request.relation,
            weight=1.0,
            metadata={},
        )

        await self.storage.upsert_graph(
            nodes=[],
            edges=[edge],
            tenant_id=request.tenant_id,
        )

        logger.info(
            f"Created KG relation: {request.source_id} -[{request.relation}]-> {request.target_id}"
        )
        return brain_pb2.CreateKGRelationResponse(success=True)

    async def SubmitVerification(self, request, context):
        """Write enrichment results from Gardener."""
        import json

        try:
            enrichment = json.loads(request.enrichment_json)
        except json.JSONDecodeError:
            enrichment = {}

        # TODO: Update product with enrichment
        logger.info(f"Received verification for {request.id}: {enrichment}")
        return brain_pb2.VerificationAck(success=True)

    async def UpsertDealerProduct(self, request, context):
        """Upsert dealer product from Harvester.

        This is the main entry point for product data from suppliers.
        """
        from google.protobuf.json_format import MessageToDict

        # Convert params Struct to dict
        params_dict = (
            MessageToDict(request.params, preserving_proto_field_name=True)
            if request.params
            else {}
        )

        product_data = {
            "tenant_id": request.tenant_id,
            "dealer_code": request.dealer_code,
            "dealer_name": request.dealer_name,
            "sku": request.sku,
            "name": request.name,
            "category": request.category,
            "brand_name": request.brand_name,
            "quantity": request.quantity,
            "price_retail": request.price_retail,
            "currency": request.currency,
            "params": params_dict,
            "status": request.status or "raw",
        }

        try:
            if hasattr(self.storage, "upsert_dealer_product"):
                product_id = await self.storage.upsert_dealer_product(**product_data)
            else:
                # Fallback: use generic upsert
                logger.warning("Using fallback upsert_dealer_product implementation")
                product_id = hash(f"{request.dealer_code}:{request.sku}") % (2**31)
                # Store as a knowledge node for now
                await self.storage.add_episode(
                    id=f"product:{request.dealer_code}:{request.sku}",
                    user_id="harvester",
                    tenant_id=request.tenant_id,
                    session_id=request.trace_id or "harvest",
                    content=f"{request.name} | {request.category} | {request.brand_name}",
                    metadata=product_data,
                )

            logger.info(
                f"Upserted dealer product: {request.dealer_code}/{request.sku} -> {product_id}"
            )
            return brain_pb2.UpsertDealerProductResponse(
                success=True,
                product_id=product_id,
                message="OK",
            )
        except Exception as e:
            logger.error(f"UpsertDealerProduct failed: {e}")
            return brain_pb2.UpsertDealerProductResponse(
                success=False,
                product_id=0,
                message=str(e),
            )


# Commerce Service - extends Brain with product operations
try:
    from contextcore import commerce_pb2_grpc

    class CommerceService(commerce_pb2_grpc.CommerceServiceServicer):
        """Commerce-specific gRPC service for product operations.

        Delegates to shared storage/logic from BrainService.
        """

        def __init__(self, brain_service: BrainService):
            self._brain = brain_service

        async def GetProducts(self, request, context):
            return await self._brain.GetProducts(request, context)

        async def UpdateEnrichment(self, request, context):
            return await self._brain.UpdateEnrichment(request, context)

        async def GetPendingVerifications(self, request, context):
            async for item in self._brain.GetPendingVerifications(request, context):
                yield item

        async def SubmitVerification(self, request, context):
            return await self._brain.SubmitVerification(request, context)

        async def UpsertDealerProduct(self, request, context):
            return await self._brain.UpsertDealerProduct(request, context)

        async def GetProduct(self, request, context):
            """Placeholder for single product retrieval."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

        async def UpdateProduct(self, request, context):
            """Placeholder for product update."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

        async def TriggerHarvest(self, request, context):
            """Placeholder for harvest trigger."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

    _HAS_COMMERCE = True
except ImportError:
    _HAS_COMMERCE = False


async def serve():
    # Setup logging from SharedConfig
    config = load_shared_config_from_env()
    setup_logging(config=config, service_name="contextbrain")

    server = grpc.aio.server()

    brain = BrainService()
    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)

    # Register Commerce service if available
    if _HAS_COMMERCE:
        commerce_pb2_grpc.add_CommerceServiceServicer_to_server(CommerceService(brain), server)
        logger.info("Commerce Service registered")

    port = os.getenv("BRAIN_PORT", "50051")
    server.add_insecure_port(f"[::]:{port}")

    logger.info(f"Unified Brain Service starting on :{port} (Async Mode)")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(serve())
