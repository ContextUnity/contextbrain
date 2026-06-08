"""Keyword extraction for Knowledge Base enrichment."""

from __future__ import annotations

import re

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_json_dict, as_json_dict_map, as_str_list
from contextunity.core.types import JsonDict

logger = get_contextunit_logger(__name__)


class KeywordExtractor:
    """Modular keyword extraction for Knowledge Base enrichment."""

    taxonomy: JsonDict | None
    _compiled_keywords: set[str] | None

    def __init__(self, taxonomy: JsonDict | None = None) -> None:
        self.taxonomy = taxonomy
        self._compiled_keywords = None
        if taxonomy:
            self._compile_taxonomy()

    def _compile_taxonomy(self) -> None:
        keywords: set[str] = set()
        root = as_json_dict(self.taxonomy)
        categories = as_json_dict_map(root.get("categories"))
        for cat_data in categories.values():
            kws = as_str_list(cat_data.get("keywords"))
            keywords.update(kw.lower() for kw in kws)
        self._compiled_keywords = keywords

    def extract(self, text: str, limit: int = 10) -> list[str]:
        text_lc = text.lower()
        found: list[str] = []

        if self._compiled_keywords:
            for kw in self._compiled_keywords:
                if kw in text_lc and re.search(rf"\b{re.escape(kw)}\b", text_lc):
                    found.append(kw)

        if len(found) < limit:
            words = as_str_list(re.findall(r"\b\w{5,}\b", text_lc))
            for word in words:
                if word not in found:
                    found.append(word)
                if len(found) >= limit:
                    break

        return found[:limit]
