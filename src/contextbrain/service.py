import os
from concurrent import futures

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


class BrainService(brain_pb2_grpc.BrainServiceServicer):
    """
    Unified implementation of the Brain gRPC service.
    """

    def __init__(self):
        dsn = os.getenv("BRAIN_DATABASE_URL", "postgresql://user:pass@localhost:5432/brain")
        self.storage = PostgresKnowledgeStore(dsn=dsn)
        self.duckdb = DuckDBStore()  # Analytical layer

    async def QueryMemory(self, request, context):
        """Hybrid search (Vector + Text) for Relevant Knowledge."""
        query_text = request.payload.get("content", "")
        # Dummy embedding for now
        query_vec = [0.1] * 768

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
        payload = request.payload
        await self.storage.upsert_taxonomy(
            tenant_id=payload.get("tenant_id", "default"),
            domain=payload.get("domain"),
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
        yield brain_pb2.ContextUnit(payload={"status": "empty"}, modality=0)


def serve():
    # Setup logging from SharedConfig
    config = load_shared_config_from_env()
    setup_logging(config=config, service_name="contextbrain")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    brain_pb2_grpc.add_BrainServiceServicer_to_server(BrainService(), server)
    server.add_insecure_port("[::]:50051")
    logger.info("Unified Brain Service starting on :50051")
    server.start()
    server.wait_for_termination()
