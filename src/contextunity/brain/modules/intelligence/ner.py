"""Named-entity extraction for Knowledge Base enrichment."""

from __future__ import annotations

import importlib
import re
from collections.abc import Iterable
from typing import Protocol, TypeGuard

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_float, as_int, as_json_dict_list, as_str
from contextunity.core.parsing import json_loads
from pydantic import BaseModel

logger = get_contextunit_logger(__name__)


class Entity(BaseModel):
    """Named entity extracted from text."""

    text: str
    label: str
    start: int = 0
    end: int = 0
    confidence: float = 1.0


class _SpacyEntity(Protocol):
    text: str
    label_: str
    start_char: int
    end_char: int


class _SpacyDoc(Protocol):
    @property
    def ents(self) -> Iterable[_SpacyEntity]: ...


class _SpacyLanguage(Protocol):
    def __call__(self, text: str) -> _SpacyDoc: ...


class _LlmGenerateProvider(Protocol):
    async def generate(self, prompt: str) -> str: ...


def _is_spacy_language(value: object) -> TypeGuard[_SpacyLanguage]:
    return callable(value)


class EntityExtractor:
    """Modular intelligence component for knowledge enrichment."""

    default_mode: str
    _spacy_nlp: _SpacyLanguage | None

    def __init__(self, default_mode: str = "spacy") -> None:
        self.default_mode = default_mode
        self._spacy_nlp = None

    async def extract(
        self,
        text: str,
        mode: str | None = None,
        llm_provider: _LlmGenerateProvider | None = None,
    ) -> list[Entity]:
        active_mode = mode or self.default_mode

        if active_mode == "spacy":
            return self._extract_spacy(text)
        if active_mode == "gateway" and llm_provider:
            return await self._extract_llm(text, llm_provider)
        if active_mode == "combined":
            local = self._extract_spacy(text)
            patterns = self._extract_patterns(text)
            return list(set(local + patterns))

        return self._extract_patterns(text)

    def _ensure_spacy(self) -> _SpacyLanguage | None:
        if self._spacy_nlp is not None:
            return self._spacy_nlp
        try:
            spacy_mod = importlib.import_module("spacy")
            loader: object = getattr(spacy_mod, "load", None)
            if not callable(loader):
                return None
            loaded: object = loader("uk_core_news_sm")
            if not _is_spacy_language(loaded):
                return None
            self._spacy_nlp = loaded
            return self._spacy_nlp
        except Exception:
            return None

    def _extract_spacy(self, text: str) -> list[Entity]:
        nlp = self._ensure_spacy()
        if nlp is None:
            return []

        doc = nlp(text)
        return [
            Entity(
                text=ent.text,
                label=ent.label_,
                start=ent.start_char,
                end=ent.end_char,
            )
            for ent in doc.ents
        ]

    def _extract_patterns(self, text: str) -> list[Entity]:
        entities: list[Entity] = []
        for match in re.finditer(r"\b[A-Z0-9-]{6,}\b", text):
            entities.append(
                Entity(text=match.group(), label="SKU", start=match.start(), end=match.end())
            )

        for match in re.finditer(r"\b\d+[\.,]\d+\s*(?:грн|UAH|\$|€)\b", text):
            entities.append(
                Entity(text=match.group(), label="PRICE", start=match.start(), end=match.end())
            )

        return entities

    async def _extract_llm(self, text: str, provider: _LlmGenerateProvider) -> list[Entity]:
        prompt = (
            f"Extract persons, organizations and products from: {text}. "
            'Return JSON list of {"text", "label"}.'
        )
        try:
            raw_res = await provider.generate(prompt)
            rows = as_json_dict_list(json_loads(raw_res))
            return [
                Entity(
                    text=as_str(row.get("text")),
                    label=as_str(row.get("label")),
                    start=as_int(row.get("start")),
                    end=as_int(row.get("end")),
                    confidence=as_float(row.get("confidence"), default=1.0),
                )
                for row in rows
            ]
        except Exception as exc:
            logger.error("LLM Extraction failed: %s", exc)
            return []
