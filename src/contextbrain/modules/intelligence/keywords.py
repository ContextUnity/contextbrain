import logging
import re
from typing import List, Optional, Set

logger = logging.getLogger(__name__)


class KeywordExtractor:
    """
    Modular keyword extraction for Knowledge Base enrichment.
    """

    def __init__(self, taxonomy: Optional[dict] = None):
        self.taxonomy = taxonomy
        self._compiled_keywords: Optional[Set[str]] = None
        if taxonomy:
            self._compile_taxonomy()

    def _compile_taxonomy(self):
        """Pre-compile taxonomy keywords for faster matching."""
        keywords = set()
        cats = self.taxonomy.get("categories", {})
        if isinstance(cats, dict):
            for cat_data in cats.values():
                if isinstance(cat_data, dict):
                    kws = cat_data.get("keywords", [])
                    keywords.update([str(k).lower() for k in kws])
        self._compiled_keywords = keywords

    def extract(self, text: str, limit: int = 10) -> List[str]:
        """
        Extract keywords from text.
        If taxonomy is provided, it performs matching first.
        """
        text_lc = text.lower()
        found = []

        # 1. Taxonomy Match (Precision)
        if self._compiled_keywords:
            for kw in self._compiled_keywords:
                if kw in text_lc:
                    # Simple boundary check
                    if re.search(rf"\b{re.escape(kw)}\b", text_lc):
                        found.append(kw)

        # 2. Simple Heuristics (Recall)
        # In a real scenario, this would use Rake, TextRank, or LLM
        if len(found) < limit:
            # Fallback to meaningful words (naive)
            words = re.findall(r"\b\w{5,}\b", text_lc)
            for w in words:
                if w not in found:
                    found.append(w)
                if len(found) >= limit:
                    break

        return found[:limit]
