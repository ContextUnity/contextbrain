import json
import logging
import re
from typing import Any, List, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Entity(BaseModel):
    text: str
    label: str
    start: int = 0
    end: int = 0
    confidence: float = 1.0


class EntityExtractor:
    """
    Modular Intelligence component for Knowledge enrichment.
    """

    def __init__(self, default_mode: str = "spacy"):
        self.default_mode = default_mode
        self._spacy_nlp = None

    async def extract(
        self, text: str, mode: Optional[str] = None, llm_provider: Optional[Any] = None
    ) -> List[Entity]:
        """
        Extract entities. Mode can be overridden per call.
        """
        mode = mode or self.default_mode

        if mode == "spacy":
            return self._extract_spacy(text)
        elif mode == "gateway" and llm_provider:
            return await self._extract_llm(text, llm_provider)
        elif mode == "combined":
            local = self._extract_spacy(text)
            # Add basic patterns
            patterns = self._extract_patterns(text)
            return list(set(local + patterns))  # Deduplicated

        return self._extract_patterns(text)

    def _extract_spacy(self, text: str) -> List[Entity]:
        if not self._spacy_nlp:
            try:
                import spacy

                self._spacy_nlp = spacy.load("uk_core_news_sm")
            except Exception:
                return []

        doc = self._spacy_nlp(text)
        return [
            Entity(text=ent.text, label=ent.label_, start=ent.start_char, end=ent.end_char)
            for ent in doc.ents
        ]

    def _extract_patterns(self, text: str) -> List[Entity]:
        entities = []
        # SKU Pattern
        for m in re.finditer(r"\b[A-Z0-9-]{6,}\b", text):
            entities.append(Entity(text=m.group(), label="SKU", start=m.start(), end=m.end()))

        # Price Pattern
        for m in re.finditer(r"\b\d+[\.,]\d+\s*(?:грн|UAH|\$|€)\b", text):
            entities.append(Entity(text=m.group(), label="PRICE", start=m.start(), end=m.end()))

        return entities

    async def _extract_llm(self, text: str, provider: Any) -> List[Entity]:
        """High-reasoning extraction via LLM provider (Gateway)."""
        prompt = f'Extract persons, organizations and products from: {text}. Return JSON list of {{"text", "label"}}.'
        try:
            # Assumes provider has a simple 'generate' interface
            raw_res = await provider.generate(prompt)
            data = json.loads(raw_res)
            return [Entity(**item) for item in data]
        except Exception as e:
            logger.error(f"LLM Extraction failed: {e}")
            return []
