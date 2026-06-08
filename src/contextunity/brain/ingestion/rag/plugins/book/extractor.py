"""PDF extraction component for Book plugin."""

from __future__ import annotations

from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.types import JsonDict

from ...protocols import (
    fitz_get_toc,
    fitz_is_available,
    fitz_open,
    fitz_page_count,
    fitz_page_text,
    pymupdf4llm_to_markdown,
    toc_entry_to_json,
)

logger = get_contextunit_logger(__name__)


class PDFExtractor:
    """Handles PDF parsing and table of contents extraction."""

    def __init__(self) -> None:
        """Initialize a new instance of PDFExtractor."""
        self._fitz_available: bool = fitz_is_available()

    def extract_toc(self, pdf_path: Path) -> list[JsonDict]:
        """Extract table of contents from PDF.

        Args:
            pdf_path (Path): The pdf path parameter.

        Returns:
            list[JsonDict]: A list of list[JsonDict].
        """
        if not self._fitz_available:
            return []

        try:
            chapters: list[JsonDict] = []
            for level, title, page in fitz_get_toc(pdf_path):
                chapters.append(toc_entry_to_json(level, title, page))
            return chapters

        except Exception as e:
            logger.warning("TOC extraction failed for %s: %s", pdf_path, e)
            return []

    def extract_text_with_pymupdf4llm(self, pdf_path: Path) -> str:
        """Extract text using pymupdf4llm for better formatting.

        Args:
            pdf_path (Path): The pdf path parameter.

        Returns:
            str: The resulting string value.
        """
        try:
            return pymupdf4llm_to_markdown(pdf_path)

        except ImportError:
            logger.warning("pymupdf4llm not available, using basic extraction")
            return self._extract_text_basic(pdf_path)
        except Exception as e:
            logger.error("PDF extraction failed for %s: %s", pdf_path, e)
            return ""

    def _extract_text_basic(self, pdf_path: Path) -> str:
        """Basic text extraction fallback.

        Args:
            pdf_path (Path): The pdf path parameter.

        Returns:
            str: The resulting string value.
        """
        if not self._fitz_available:
            return ""

        try:
            doc = fitz_open(pdf_path)
            try:
                parts: list[str] = []
                for page_index in range(doc.page_count):
                    parts.append(fitz_page_text(doc, page_index))
                return "\n".join(parts)
            finally:
                _ = doc.close()

        except Exception as e:
            logger.error("Basic PDF extraction failed: %s", e)
            return ""

    def get_page_count(self, pdf_path: Path) -> int:
        """Get total page count of PDF.

        Args:
            pdf_path (Path): The pdf path parameter.

        Returns:
            int: The resulting integer value.
        """
        if not self._fitz_available:
            return 0

        try:
            return fitz_page_count(pdf_path)
        except Exception:
            return 0
