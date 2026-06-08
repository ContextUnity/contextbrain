"""Main Book transformer that orchestrates all book processing components."""

from __future__ import annotations

import re
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import (
    as_json_dict,
    as_json_dict_list,
    as_str,
    as_str_list,
    str_list_as_json,
)
from contextunity.core.types import JsonDict, JsonValue, is_object_list

from contextunity.brain.core import BrainConfig
from contextunity.brain.core.types import StructData

from .analyzer import BookAnalyzer
from .extractor import PDFExtractor
from .normalizer import ContentNormalizer

logger = get_contextunit_logger(__name__)


class BookTransformer:
    """Main book transformer that orchestrates all book processing."""

    def __init__(self, core_cfg: BrainConfig) -> None:
        """Initialize a new instance of BookTransformer.

        Args:
            core_cfg (BrainConfig): The core cfg parameter.
        """
        self.core_cfg: BrainConfig = core_cfg
        self.extractor: PDFExtractor = PDFExtractor()
        self.normalizer: ContentNormalizer = ContentNormalizer()
        self.analyzer: BookAnalyzer = BookAnalyzer(core_cfg)

    def transform_pdf(self, pdf_path: Path) -> JsonDict:
        """Transform a single PDF into processed content.

        Args:
            pdf_path (Path): The pdf path parameter.

        Returns:
            JsonDict: A dictionary containing the results.
        """
        try:
            # Extract TOC
            toc = self.extractor.extract_toc(pdf_path)
            logger.info(f"Extracted {len(toc)} chapters from TOC")

            # Extract text
            raw_text = self.extractor.extract_text_with_pymupdf4llm(pdf_path)
            logger.info(f"Extracted {len(raw_text)} characters of text")

            # Normalize content
            cleaned_text = self.normalizer.clean_text(raw_text)
            normalized_text = self.normalizer.normalize_unicode(cleaned_text)

            # Split into chunks
            chunks = self.normalizer.split_into_chunks(normalized_text)

            # Analyze chunks
            analyses = self.analyzer.analyze_batch(chunks)

            # Split by chapters if TOC available
            chapter_chunks = self.analyzer.chunk_by_chapters(normalized_text, toc)

            chapters_json: list[JsonValue] = list(toc)
            chapter_chunks_json: list[JsonValue] = list(chapter_chunks)

            return {
                "text": normalized_text,
                "chapters": chapters_json,
                "chunks": str_list_as_json(chunks),
                "analyses": list(analyses),
                "chapter_chunks": chapter_chunks_json,
                "metadata": {
                    "page_count": self.extractor.get_page_count(pdf_path),
                    "estimated_reading_time": self.analyzer.estimate_reading_time(normalized_text),
                    "chunk_count": len(chunks),
                },
            }

        except Exception as e:
            logger.error("Book transformation failed for %s: %s", pdf_path, e)
            return {
                "text": "",
                "chapters": [],
                "chunks": [],
                "analyses": [],
                "chapter_chunks": [],
                "metadata": {},
                "error": str(e),
            }

    def transform_multiple_pdfs(self, pdf_paths: list[Path]) -> list[JsonDict]:
        """Transform multiple PDFs.

        Args:
            pdf_paths (list[Path]): The pdf paths parameter.

        Returns:
            list[JsonDict]: A list of list[JsonDict].
        """
        results: list[JsonDict] = []

        for pdf_path in pdf_paths:
            logger.info("Processing book: %s", pdf_path.name)
            result = self.transform_pdf(pdf_path)
            # Keep metadata consistent with ingestion schema (reader expects book_title).
            book_title_raw = pdf_path.stem.replace("_", " ").replace("-", " ").strip()
            book_title_raw = re.sub(r"\s+", " ", book_title_raw)
            result["book_title"] = book_title_raw.title() if book_title_raw else pdf_path.stem
            results.append(result)

        return results

    def create_structured_records(
        self, transformation_result: JsonDict, taxonomy: StructData | None = None
    ) -> list[JsonDict]:
        """Create structured records from transformation result.

        Args:
            transformation_result (JsonDict): The transformation result parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            list[JsonDict]: A list of list[JsonDict].
        """
        records: list[JsonDict] = []

        chunks = as_str_list(transformation_result.get("chunks"))
        analyses = as_json_dict_list(transformation_result.get("analyses"))
        metadata_base = as_json_dict(transformation_result.get("metadata"))

        for i, chunk in enumerate(chunks):
            analysis = analyses[i] if i < len(analyses) else {}
            record_metadata: JsonDict = dict(metadata_base)
            record_metadata["themes"] = str_list_as_json(as_str_list(analysis.get("themes")))
            record_metadata["topics"] = str_list_as_json(as_str_list(analysis.get("topics")))
            record_metadata["book_title"] = as_str(transformation_result.get("book_title"))
            record: JsonDict = {
                "content": chunk,
                "chunk_id": i,
                "source_type": "book",
                "metadata": record_metadata,
            }

            # Add taxonomy if available
            if taxonomy:
                record["taxonomy_categories"] = str_list_as_json(
                    self._map_to_taxonomy(chunk, taxonomy)
                )

            records.append(record)

        return records

    def _map_to_taxonomy(self, content: str, taxonomy: StructData) -> list[str]:
        """Map content to taxonomy categories.

        Args:
            content (str): The content parameter.
            taxonomy (StructData): The taxonomy parameter.

        Returns:
            list[str]: A list of list[str].
        """
        categories_raw = taxonomy.get("categories", [])
        if not is_object_list(categories_raw):
            return []
        content_lower = content.lower()

        matched: list[str] = []
        for category_obj in categories_raw:
            category = category_obj if isinstance(category_obj, str) else ""
            if not category:
                continue
            category_lower = category.lower()
            if any(word in content_lower for word in category_lower.split()):
                matched.append(category)

        return matched
