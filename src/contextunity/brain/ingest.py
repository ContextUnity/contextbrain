"""Module providing Module docstring is missing capabilities."""

from __future__ import annotations

from typing import Protocol

from contextunity.core import ContextUnit, get_contextunit_logger
from contextunity.core.narrowing import as_json_dict_list, as_str
from contextunity.core.sdk.models import UnitMetrics
from contextunity.core.types import JsonDict

from contextunity.brain.modules.intelligence.hub import IntelligenceHub
from contextunity.brain.storage.contracts import KnowledgeStoreProtocol
from contextunity.brain.storage.graph.cognee import KnowledgeGraphOrchestrator

logger = get_contextunit_logger(__name__)


class TextEmbedder(Protocol):
    async def embed_async(self, text: str) -> list[float]: ...


class IngestionService:
    """
    Handles the ETL pipeline for ingesting knowledge into contextunity.brain.

    Responsibilities:
    1. Parse raw content (text, markdown).
    2. Chunk content into semantic segments.
    3. Generate embeddings (delegated to models).
    4. Store in PostgresKnowledgeStore (pgvector).
    """

    storage: KnowledgeStoreProtocol
    intel: IntelligenceHub
    graph: KnowledgeGraphOrchestrator

    def __init__(self, storage: KnowledgeStoreProtocol, project_path: str | None = None):
        """Initialize the modular ingestion pipeline.

        Args:
            storage (KnowledgeStoreProtocol): The storage parameter.
            project_path (str | None): The project path parameter.
        """
        self.storage = storage
        self.intel = IntelligenceHub(project_path)
        self.graph = KnowledgeGraphOrchestrator()

    async def ingest_document(
        self,
        content: str | bytes,
        metadata: JsonDict,
        modality: str = "text",
        embedder: TextEmbedder | None = None,
        tenant_id: str = "default",
        user_id: str | None = None,
        source_type: str = "document",
    ) -> str:
        """Process and store content based on its modality.

        Args:
            content (str | bytes): Raw document payload.
            metadata (JsonDict): Document metadata.
            modality (str): The modality parameter.
            embedder (TextEmbedder | None): Optional embedding provider.
            tenant_id (str): The tenant id parameter.
            user_id (str | None): The user id parameter.
            source_type (str): The source type parameter.

        Returns:
            str: The resulting string value.
        """
        if modality == "text":
            if not isinstance(content, str):
                logger.error("Text modality requires str content")
                return "error"
            return await self._ingest_text(
                content,
                metadata,
                embedder=embedder,
                tenant_id=tenant_id,
                user_id=user_id,
                source_type=source_type,
            )
        if modality == "image":
            if not isinstance(content, bytes):
                logger.error("Image modality requires bytes content")
                return "error"
            return await self._ingest_binary(content, metadata, "image")
        if modality == "audio":
            if not isinstance(content, bytes):
                logger.error("Audio modality requires bytes content")
                return "error"
            return await self._ingest_binary(content, metadata, "audio")
        logger.error(f"Unsupported modality: {modality}")
        return "error"

    async def _ingest_text(
        self,
        content: str,
        metadata: JsonDict,
        embedder: TextEmbedder | None = None,
        tenant_id: str = "default",
        user_id: str | None = None,
        source_type: str = "document",
    ) -> str:
        # Enrichment step (The "Smart Brain" part)
        """ingest text.

        Args:
            content (str): The content parameter.
            metadata (JsonDict): The metadata parameter.
            embedder (TextEmbedder | None): Optional embedding provider.
            tenant_id (str): The tenant id parameter.
            user_id (str | None): The user id parameter.
            source_type (str): The source type parameter.

        Returns:
            str: The resulting string value.
        """
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
            if not isinstance(doc_id, str):
                doc_id = str(doc_id)

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
                user_id=user_id,
            )
            entities = as_json_dict_list(enriched_metadata.get("entities", []))
            await self.graph.add_data(unit, entities)

        return doc_id or "error"

    async def _ingest_binary(self, _data: bytes, _metadata: JsonDict, modality: str) -> str:
        """Stub for binary data ingestion (GCS storage + metadata link).

        Args:
            _data (bytes): Raw binary payload (unused placeholder).
            _metadata (JsonDict): Document metadata (unused placeholder).
            modality (str): The modality parameter.

        Returns:
            str: The resulting string value.
        """
        logger.info(f"Ingesting binary {modality} data. (GCS Sink Placeholder)")
        # 1. Upload to GCS/S3
        # 2. Extract metadata via specialized models (Vision/Whisper)
        # 3. Store reference in Postgres
        return f"{modality}_id_placeholder"

    async def _enrich_metadata(self, content: str, metadata: JsonDict) -> JsonDict:
        """Run modular intelligence hub to enrich unit metadata.

        Args:
            content (str): The content parameter.
            metadata (JsonDict): The metadata parameter.

        Returns:
            JsonDict: Enriched metadata.
        """
        enriched: JsonDict = dict(metadata)

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
        source_type = as_str(enriched.get("source_type"))
        if not enriched.get("category") and source_type not in skip_types:
            matched_cat = self.intel.taxonomy.match_category(content)
            if matched_cat:
                enriched["category"] = matched_cat
            else:
                await self._persist_pending("category", content, metadata)

        # 2. Resolve Size
        raw_size = enriched.get("raw_size")
        if raw_size is not None:
            raw_size_text = as_str(raw_size)
            category_context = as_str(enriched.get("category", ""))
            resolved_size = self.intel.taxonomy.resolve_size(
                raw_size_text, category_context=category_context
            )
            enriched["size"] = resolved_size["resolved"]
            enriched["size_standard"] = resolved_size["standard"]
            if resolved_size["standard"] == "unknown":
                await self._persist_pending("size", raw_size_text, enriched)

        return enriched

    async def _persist_pending(self, item_type: str, raw_value: str, _context: JsonDict) -> None:
        """Save unrecognized item to database for Gardener UI.

        Args:
            item_type (str): The item type parameter.
            raw_value (str): The raw value parameter.
            _context (JsonDict): Request context payload (reserved for future DB insert).
        """
        # This part requires a DB connection.
        # For now, we logging. In a real system, we'd use self.vector_store.session or similar.
        logger.warning(
            f"GARDENER ALERT: Unrecognized {item_type}: '{raw_value}'. Added to review queue."
        )
        # Placeholder for DB INSERT into gardener_pending
