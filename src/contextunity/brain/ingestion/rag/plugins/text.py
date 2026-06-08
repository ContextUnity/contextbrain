"""Generic text/knowledge ingestion plugin."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str_list
from typing_extensions import override

from contextunity.brain.core.types import StructData

from ..core.loaders import FileLoaderMixin, LoadedFile
from ..core.plugins import IngestionPlugin
from ..core.registry import register_plugin
from ..core.types import (
    GraphEnrichmentResult,
    KnowledgeStructData,
    RawData,
    ShadowRecord,
)
from ..core.utils import (
    build_enriched_input_text,
    get_graph_enrichment,
    load_taxonomy_safe,
    normalize_ambiguous_unicode,
)
from ..settings import RagIngestionConfig
from ..utils.records import generate_id

logger = get_contextunit_logger(__name__)


@register_plugin("knowledge")
class TextPlugin(IngestionPlugin, FileLoaderMixin):
    """Plugin for processing generic text/knowledge files.

    This content is for RAG context only, NOT for UI citations.
    struct_data will be empty to prevent citation card rendering.
    """

    @property
    @override
    def source_type(self) -> str:
        """Source type.

        Returns:
            str: The resulting string value.
        """
        return "knowledge"

    @override
    def load(self, assets_path: str) -> list[RawData]:
        """Load .txt or .md files as-is.

        Args:
            assets_path (str): The assets path parameter.

        Returns:
            list[RawData]: A list of list[RawData].
        """
        if not (source_dir := self._resolve_source_dir(assets_path)):
            return []

        def to_raw_data(loaded: LoadedFile) -> RawData:
            """To raw data.

            Args:
                loaded (LoadedFile): The loaded parameter.

            Returns:
                RawData: An instance of RawData.
            """
            return RawData(
                content=normalize_ambiguous_unicode(loaded.content),
                source_type="knowledge",
                metadata={"title": loaded.path.stem},
            )

        return [
            to_raw_data(f) for f in self._load_text_files(source_dir, extensions=(".txt", ".md"))
        ]

    @override
    def transform(
        self,
        data: list[RawData],
        enrichment_func: Callable[[str], GraphEnrichmentResult],
        taxonomy_path: Path | None = None,
        config: RagIngestionConfig | None = None,
        **kwargs: object,
    ) -> list[ShadowRecord]:
        """Transform text data with taxonomy term matching for enhanced retrieval.

        Args:
            data (list[RawData]): The raw data dictionary or object.
            enrichment_func (Callable[[str], GraphEnrichmentResult]): The enrichment func parameter.
            taxonomy_path (Path | None): The taxonomy path parameter.
            config (RagIngestionConfig | None): The configuration settings dict or object.

        Returns:
            list[ShadowRecord]: A list of list[ShadowRecord].
        """
        shadow_records: list[ShadowRecord] = []

        _ = config, kwargs

        # Load taxonomy for term matching
        taxonomy = load_taxonomy_safe(taxonomy_path)

        for raw in data:
            title = str(raw.metadata.get("title", "Knowledge"))

            # Chunk by paragraphs
            paragraphs = [p.strip() for p in raw.content.split("\n\n") if p.strip()]
            current_chunk = ""

            for para in paragraphs:
                if len(current_chunk) + len(para) > 1000 and current_chunk:
                    record = self._create_knowledge_record(
                        current_chunk,
                        title,
                        enrichment_func,
                        taxonomy,
                        initial_keywords=raw.metadata.get("keywords", []),
                    )
                    shadow_records.append(record)
                    current_chunk = para
                else:
                    current_chunk += "\n\n" + para if current_chunk else para

            # Handle remaining chunk
            if current_chunk:
                record = self._create_knowledge_record(
                    current_chunk,
                    title,
                    enrichment_func,
                    taxonomy,
                    initial_keywords=raw.metadata.get("keywords", []),
                )
                shadow_records.append(record)

        return shadow_records

    def _create_knowledge_record(
        self,
        chunk: str,
        title: str,
        enrichment_func: Callable[[str], GraphEnrichmentResult],
        taxonomy: StructData | None,
        *,
        initial_keywords: list[str] | object | None = None,
    ) -> ShadowRecord:
        """Create a ShadowRecord for a knowledge chunk.

        Args:
            chunk (str): The chunk parameter.
            title (str): The title parameter.
            enrichment_func (Callable[[str], GraphEnrichmentResult]): The enrichment func parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            ShadowRecord: An instance of ShadowRecord.
        """
        # Graph enrichment
        graph_keywords, summary, parent_categories = get_graph_enrichment(
            text=chunk, enrichment_func=enrichment_func
        )

        # Extract taxonomy terms from content
        taxonomy_terms = self._extract_taxonomy_terms(chunk, taxonomy)

        # Combine metadata + taxonomy + graph keywords, deduplicated
        base: list[str] = as_str_list(initial_keywords)
        all_keywords: list[str] = list(dict.fromkeys([*base, *taxonomy_terms, *graph_keywords]))[
            :15
        ]

        # Build input_text with natural language enrichment
        input_text = build_enriched_input_text(
            content=chunk,
            keywords=all_keywords,
            summary=summary,
            parent_categories=parent_categories,
        )

        record_id = generate_id(title, chunk[:50])

        # Generate short description: use graph summary or first sentence(s) of chunk
        description = ""
        if summary and summary.strip():
            # Use graph enrichment summary if available
            description = summary.strip()
            # Limit to ~150 chars for metadata
            if len(description) > 150:
                description = description[:147] + "..."
        else:
            # Fallback: first sentence or first 150 chars
            first_sentence = chunk.split(". ")[0].strip()
            if len(first_sentence) > 150:
                description = first_sentence[:147] + "..."
            else:
                description = first_sentence

        # struct_data with filename and description metadata
        struct_data: KnowledgeStructData = {
            "source_type": "knowledge",
            "filename": title,  # Original filename (without extension)
            "description": description,  # Short description of content
        }

        return ShadowRecord(
            id=record_id,
            input_text=input_text,
            struct_data=dict(struct_data) if struct_data else {},
            title=title,
            source_type="knowledge",
        )

    def _extract_taxonomy_terms(
        self,
        content: str,
        taxonomy: StructData | None,
    ) -> list[str]:
        """Extract matching taxonomy terms from content.

        Args:
            content (str): The content parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            list[str]: A list of list[str].
        """
        if not taxonomy:
            return []

        all_keywords = taxonomy.get("all_keywords", [])
        if not all_keywords or not isinstance(all_keywords, list):
            return []

        content_lower = content.lower()
        matches: list[tuple[str, int]] = []

        for keyword in all_keywords:
            if not isinstance(keyword, str):
                continue
            keyword_lower = keyword.lower()
            count = content_lower.count(keyword_lower)
            if count > 0:
                matches.append((keyword, count))

        # Sort by frequency, return top 10
        matches.sort(key=lambda x: x[1], reverse=True)
        return [m[0] for m in matches[:10]]
