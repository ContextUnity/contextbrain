"""Keyphrase extraction for Knowledge Base enrichment."""

from __future__ import annotations

import re

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str_list

logger = get_contextunit_logger(__name__)


class KeyphraseExtractor:
    """Modular intelligence component for identifying meaningful phrases."""

    stop_words: set[str]

    def __init__(self, stop_words: list[str] | None = None) -> None:
        self.stop_words = set(stop_words or ["і", "на", "в", "до", "з", "за"])

    def extract(self, text: str, limit: int = 5) -> list[str]:
        words = as_str_list(re.findall(r"\b\w{4,}\b", text.lower()))
        candidates = [word for word in words if word not in self.stop_words]

        counts: dict[str, int] = {}
        for word in candidates:
            counts[word] = counts.get(word, 0) + 1

        def sort_key(phrase: str) -> tuple[int, int]:
            return (counts[phrase], len(phrase))

        sorted_phrases = sorted(counts.keys(), key=sort_key, reverse=True)
        return sorted_phrases[:limit]
