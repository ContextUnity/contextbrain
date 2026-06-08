"""Taxonomy mapping component for QA plugin."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str_list
from contextunity.core.types import is_object_list

from contextunity.brain.core.types import StructData

logger = get_contextunit_logger(__name__)


class TaxonomyMapper:
    """Handles mapping of content to taxonomy categories."""

    def __init__(self, taxonomy: StructData | None = None) -> None:
        """Initialize a new instance of TaxonomyMapper.

        Args:
            taxonomy (StructData | None): The taxonomy parameter.
        """
        self.taxonomy: StructData = taxonomy or {}

    def _get_taxonomy_categories(self, taxonomy: StructData) -> list[str]:
        """Extract category names from taxonomy.

        Args:
            taxonomy (StructData): The taxonomy parameter.

        Returns:
            list[str]: A list of list[str].
        """
        categories: list[str] = []

        for key in ("categories", "topics", "subjects"):
            val = taxonomy.get(key)
            if is_object_list(val):
                categories.extend(as_str_list(val))

        return categories

    def map_to_taxonomy(self, content: str, taxonomy: StructData | None = None) -> list[str]:
        """Map content to relevant taxonomy categories.

        Args:
            content (str): The content parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            list[str]: A list of list[str].
        """
        if not taxonomy and not self.taxonomy:
            return []

        taxonomy_data = taxonomy or self.taxonomy
        categories = self._get_taxonomy_categories(taxonomy_data)

        if not categories:
            return []

        # Simple keyword matching - can be enhanced with ML
        content_lower = content.lower()
        matched_categories: list[str] = []

        for category in categories:
            category_lower = category.lower()
            # Check if category keywords appear in content
            if any(word in content_lower for word in category_lower.split()):
                matched_categories.append(category)

        return matched_categories

    def update_taxonomy(self, new_taxonomy: StructData) -> None:
        """Update the taxonomy data.

        Args:
            new_taxonomy (StructData): The new taxonomy parameter.
        """
        self.taxonomy = new_taxonomy
