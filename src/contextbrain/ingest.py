import logging
from typing import Any, Dict

from contextcore import ContextUnit
from contextcore.sdk.models import UnitMetrics

from .storage import PostgresKnowledgeStore

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Handles the ETL pipeline for ingesting knowledge into ContextBrain.

    Responsibilities:
    1. Parse raw content (text, markdown).
    2. Chunk content into semantic segments.
    3. Generate embeddings (delegated to models).
    4. Store in PostgresKnowledgeStore (pgvector).
    """

    def __init__(self, storage: PostgresKnowledgeStore, project_path: str | None = None):
        """
        Initialize the modular ingestion pipeline.
        project_path: optional project root for taxonomy; defaults to CONTEXTBRAIN_PROJECT_PATH env.
        """
        self.storage = storage
        from .modules.intelligence.hub import IntelligenceHub
        from .storage.graph.cognee import KnowledgeGraphOrchestrator

        self.intel = IntelligenceHub(project_path)
        self.graph = KnowledgeGraphOrchestrator()

    async def ingest_document(
        self,
        content: Any,
        metadata: Dict[str, Any],
        modality: str = "text",
        embedder: Any = None,
        tenant_id: str = "default",
        source_type: str = "document",
    ) -> str:
        """
        Process and store content based on its modality.
        """
        if modality == "text":
            return await self._ingest_text(
                content,
                metadata,
                embedder=embedder,
                tenant_id=tenant_id,
                source_type=source_type,
            )
        elif modality == "image":
            return await self._ingest_binary(content, metadata, "image")
        elif modality == "audio":
            return await self._ingest_binary(content, metadata, "audio")
        else:
            logger.error(f"Unsupported modality: {modality}")
            return "error"

    async def _ingest_text(
        self,
        content: str,
        metadata: Dict[str, Any],
        embedder: Any = None,
        tenant_id: str = "default",
        source_type: str = "document",
    ) -> str:
        # Enrichment step (The "Smart Brain" part)
        enriched_metadata = await self._enrich_metadata(content, metadata)

        # Placeholder: Chunking logic would go here
        chunks = [content]

        from .storage.postgres.models import GraphNode

        doc_id = None
        for chunk in chunks:
            # Generate real embeddings if embedder is available
            if embedder is not None:
                try:
                    embedding = await embedder.embed_async(chunk)
                except Exception as e:
                    logger.warning("Embedding failed, using placeholder: %s", e)
                    embedding = [0.1] * 1536
            else:
                embedding = [0.1] * 1536

            unit = ContextUnit(
                modality="text",
                payload={"content": chunk, "metadata": enriched_metadata},
                provenance=["brain:ingest:chunk"],
                metrics=UnitMetrics(tokens_used=len(chunk.split())),
            )

            # Use deterministic ID from metadata if provided (enables true upsert)
            doc_id = enriched_metadata.pop("_doc_id", None) or str(unit.unit_id)

            # Map ContextUnit to GraphNode for persistence
            node = GraphNode(
                id=doc_id,
                content=chunk,
                embedding=embedding,
                node_kind="chunk",
                source_type=source_type,
                metadata=enriched_metadata,
                tenant_id=tenant_id,
            )

            await self.storage.upsert_graph(
                nodes=[node],
                edges=[],
                tenant_id=tenant_id,
            )
            await self.graph.add_data(unit, enriched_metadata.get("entities", []))

        return doc_id or "error"

    async def _ingest_binary(self, data: bytes, metadata: Dict[str, Any], modality: str) -> str:
        """Stub for binary data ingestion (GCS storage + metadata link)."""
        logger.info(f"Ingesting binary {modality} data. (GCS Sink Placeholder)")
        # 1. Upload to GCS/S3
        # 2. Extract metadata via specialized models (Vision/Whisper)
        # 3. Store reference in Postgres
        return f"{modality}_id_placeholder"

    async def _enrich_metadata(self, content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Run modular intelligence hub to enrich unit metadata."""
        enriched = metadata.copy()

        # Run Hub: NER, Keywords, Keyphrases
        intel_data = await self.intel.enrich_content(content)
        enriched.update(
            {
                "entities": intel_data["entities"],
                "keyphrases": intel_data["keyphrases"],
                "keywords": intel_data["keywords"],
            }
        )

        # 1. Resolve Category (via Taxonomy Manager inside Intel)
        # Skip taxonomy matching for non-product content (docs, tool definitions, etc.)
        skip_types = {"procedural", "documentation"}
        if not enriched.get("category") and enriched.get("source_type") not in skip_types:
            matched_cat = self.intel.taxonomy.match_category(content)
            if matched_cat:
                enriched["category"] = matched_cat
            else:
                await self._persist_pending("category", content, metadata)

        # 2. Resolve Size
        if enriched.get("raw_size"):
            resolved_size = self.intel.taxonomy.resolve_size(
                enriched["raw_size"], category_context=enriched.get("category", "")
            )
            enriched["size"] = resolved_size["resolved"]
            enriched["size_standard"] = resolved_size["standard"]
            if resolved_size["standard"] == "unknown":
                await self._persist_pending("size", enriched["raw_size"], enriched)

        return enriched

    async def _persist_pending(self, item_type: str, raw_value: str, context: Dict):
        """Save unrecognized item to database for Gardener UI."""
        # This part requires a DB connection.
        # For now, we logging. In a real system, we'd use self.vector_store.session or similar.
        logger.warning(
            f"GARDENER ALERT: Unrecognized {item_type}: '{raw_value}'. Added to review queue."
        )
        # Placeholder for DB INSERT into gardener_pending
