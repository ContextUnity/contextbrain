import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class IntelligenceHub:
    """
    Coordinator for all smart features in the Knowledge Hub.

    Uses spaCy NER and KeyBERT topic extraction when available,
    falling back to legacy regex-based extractors.
    """

    def __init__(self, project_path: str | None = None):
        # Modern NLP enrichment (spaCy + KeyBERT)
        self._nlp_enricher = None
        try:
            from contextbrain.service.nlp import NLPEnricher

            self._nlp_enricher = NLPEnricher.get_instance()
            caps = self._nlp_enricher.capabilities
            if caps["ner"] or caps["topics"]:
                logger.info(
                    "NLP enrichment active: NER=%s, Topics=%s",
                    caps["ner"],
                    caps["topics"],
                )
        except Exception as e:
            logger.debug("NLP enrichment unavailable: %s", e)

        # Legacy extractors (fallback)
        from .keyphrases import KeyphraseExtractor
        from .keywords import KeywordExtractor
        from .ner import EntityExtractor

        self._legacy_ner = EntityExtractor()
        self._legacy_keyphrases = KeyphraseExtractor()

        from .taxonomy_manager import TaxonomyManager

        if not project_path:
            from contextbrain.core import get_core_config

            project_path = get_core_config().project_path
        self.taxonomy = TaxonomyManager(project_path or "")
        self._legacy_keywords = KeywordExtractor(taxonomy=self.taxonomy.categories)

    async def enrich_content(self, text: str) -> Dict[str, Any]:
        """
        Runs intelligence modules over the content.

        Priority: spaCy/KeyBERT → legacy extractors (fallback).
        """
        # Try modern NLP pipeline first
        if self._nlp_enricher:
            try:
                result = self._nlp_enricher.enrich(text)
                entities = [
                    {"text": e.text, "label": e.label, "start": e.start, "end": e.end}
                    for e in result.entities
                ]
                # Combine KeyBERT topics with legacy keyword extraction
                keywords = result.topics or []
                if not keywords:
                    keywords = self._legacy_keywords.extract(text)

                keyphrases = self._legacy_keyphrases.extract(text)

                return {
                    "entities": entities,
                    "keyphrases": keyphrases,
                    "keywords": keywords,
                    "topics": result.topics,
                    "language": result.language,
                    "summary_signals": text[:200],
                }
            except Exception as e:
                logger.warning("NLP enrichment failed, using legacy: %s", e)

        # Legacy fallback
        entities = await self._legacy_ner.extract(text)
        keyphrases = self._legacy_keyphrases.extract(text)
        keywords = self._legacy_keywords.extract(text)

        return {
            "entities": [e.model_dump() for e in entities],
            "keyphrases": keyphrases,
            "keywords": keywords,
            "topics": [],
            "language": "",
            "summary_signals": text[:200],
        }
