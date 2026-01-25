import logging
import re
from typing import List, Optional

logger = logging.getLogger(__name__)


class KeyphraseExtractor:
    """
    Modular intelligence component for identifying meaningful phrases.
    """

    def __init__(self, stop_words: Optional[List[str]] = None):
        self.stop_words = set(stop_words or ["і", "на", "в", "до", "з", "за"])

    def extract(self, text: str, limit: int = 5) -> List[str]:
        """
        Naive implementation based on frequency and length.
        Can be upgraded to TextRank or LLM.
        """
        words = re.findall(r"\b\w{4,}\b", text.lower())
        candidates = [w for w in words if w not in self.stop_words]

        # Simple frequency count
        counts = {}
        for w in candidates:
            counts[w] = counts.get(w, 0) + 1

        # Sort by frequency and then length
        sorted_phrases = sorted(counts.keys(), key=lambda x: (counts[x], len(x)), reverse=True)
        return sorted_phrases[:limit]
