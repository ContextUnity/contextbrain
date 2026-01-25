import logging
from typing import Any, Dict

from .keyphrases import KeyphraseExtractor
from .keywords import KeywordExtractor
from .ner import EntityExtractor

logger = logging.getLogger(__name__)


class IntelligenceHub:
    """
    Coordinator for all smart features in the Knowledge Hub.
    """

    def __init__(self, project_path: str = "/home/oleksii/Projects/traverse"):
        self.ner = EntityExtractor()
        self.keyphrases = KeyphraseExtractor()

        from .taxonomy_manager import TaxonomyManager

        self.taxonomy = TaxonomyManager(project_path)
        self.keywords = KeywordExtractor(taxonomy=self.taxonomy.categories)

    async def enrich_content(self, text: str) -> Dict[str, Any]:
        """
        Runs multiple intelligence modules over the content.
        """
        entities = await self.ner.extract(text)
        keyphrases = self.keyphrases.extract(text)
        keywords = self.keywords.extract(text)

        return {
            "entities": [e.model_dump() for e in entities],
            "keyphrases": keyphrases,
            "keywords": keywords,
            "summary_signals": text[:200],  # Seed for summarization
        }
