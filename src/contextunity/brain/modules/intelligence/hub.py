"""Coordinator for Knowledge Hub intelligence modules."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import str_list_as_json
from contextunity.core.types import JsonDict, JsonValue

from contextunity.brain.service.nlp import NLPEnricher

from .keyphrases import KeyphraseExtractor
from .keywords import KeywordExtractor
from .ner import Entity, EntityExtractor
from .taxonomy_manager import TaxonomyManager

logger = get_contextunit_logger(__name__)


class IntelligenceHub:
    """
    Coordinator for all smart features in the Knowledge Hub.

    Uses spaCy NER and KeyBERT topic extraction when available,
    falling back to legacy regex-based extractors.
    """

    _nlp_enricher: NLPEnricher | None
    _legacy_ner: EntityExtractor
    _legacy_keyphrases: KeyphraseExtractor
    taxonomy: TaxonomyManager
    _legacy_keywords: KeywordExtractor

    def __init__(self, project_path: str | None = None) -> None:
        self._nlp_enricher = None
        try:
            self._nlp_enricher = NLPEnricher.get_instance()
            caps = self._nlp_enricher.capabilities
            if caps["ner"] or caps["topics"]:
                logger.info(
                    "NLP enrichment active: NER=%s, Topics=%s",
                    caps["ner"],
                    caps["topics"],
                )
        except Exception as exc:
            logger.debug("NLP enrichment unavailable: %s", exc)

        self._legacy_ner = EntityExtractor()
        self._legacy_keyphrases = KeyphraseExtractor()

        if not project_path:
            from contextunity.brain.core import get_core_config

            project_path = get_core_config().project_path
        self.taxonomy = TaxonomyManager(project_path or "")
        self._legacy_keywords = KeywordExtractor(taxonomy=self.taxonomy.categories)

    async def enrich_content(self, text: str) -> JsonDict:
        if self._nlp_enricher:
            try:
                result = self._nlp_enricher.enrich(text)
                entities: list[JsonValue] = [
                    {
                        "text": entity.text,
                        "label": entity.label,
                        "start": entity.start,
                        "end": entity.end,
                    }
                    for entity in result.entities
                ]
                keywords = result.topics or []
                if not keywords:
                    keywords = self._legacy_keywords.extract(text)

                keyphrases = self._legacy_keyphrases.extract(text)

                return {
                    "entities": entities,
                    "keyphrases": str_list_as_json(keyphrases),
                    "keywords": str_list_as_json(keywords),
                    "topics": str_list_as_json(result.topics),
                    "language": result.language,
                    "summary_signals": text[:200],
                }
            except Exception as exc:
                logger.warning("NLP enrichment failed, using legacy: %s", exc)

        entities_legacy = await self._legacy_ner.extract(text)
        keyphrases = self._legacy_keyphrases.extract(text)
        keywords = self._legacy_keywords.extract(text)

        return {
            "entities": [_entity_to_json(entity) for entity in entities_legacy],
            "keyphrases": str_list_as_json(keyphrases),
            "keywords": str_list_as_json(keywords),
            "topics": [],
            "language": "",
            "summary_signals": text[:200],
        }


def _entity_to_json(entity: Entity) -> JsonDict:
    return {
        "text": entity.text,
        "label": entity.label,
        "start": entity.start,
        "end": entity.end,
        "confidence": entity.confidence,
    }
