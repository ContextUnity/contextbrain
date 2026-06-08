"""Keyword taxonomy utilities."""

from __future__ import annotations

from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str_list
from contextunity.core.parsing import json_loads
from contextunity.core.types import JsonDict, is_json_dict

logger = get_contextunit_logger(__name__)

# Default relative to package root (packages/contextunity.brain/)
DEFAULT_KEYWORDS_PATH = Path("assets/taxonomy.json")


def load_keyword_taxonomy(path: Path | None = None) -> JsonDict | None:
    """Load keyword taxonomy.

    Args:
        path (Path | None): The filesystem path.

    Returns:
        dict | None: An instance of dict | None.
    """
    taxonomy_path = path or DEFAULT_KEYWORDS_PATH
    if not taxonomy_path.exists():
        logger.debug("No keyword taxonomy found at %s", taxonomy_path)
        return None

    try:
        payload = json_loads(taxonomy_path.read_text(encoding="utf-8"))
        if not is_json_dict(payload):
            logger.warning("Keyword taxonomy root must be a JSON object")
            return None
        logger.info("Loaded keyword taxonomy")
        return payload
    except Exception as e:
        logger.warning("Failed to load keyword taxonomy: %s", e)
        return None


def get_taxonomy_keywords(taxonomy: JsonDict | None) -> list[str]:
    """Retrieve the taxonomy keywords information.

    Args:
        taxonomy (dict | None): The taxonomy parameter.

    Returns:
        list[str]: A list of list[str].
    """
    if not taxonomy:
        return []

    if "all_keywords" in taxonomy and isinstance(taxonomy["all_keywords"], list):
        return as_str_list(taxonomy["all_keywords"])

    keywords: list[str] = []
    categories = taxonomy.get("categories", {})

    if isinstance(categories, dict):
        for cat_data in categories.values():
            if isinstance(cat_data, dict):
                kws = cat_data.get("keywords", [])
                if isinstance(kws, list):
                    keywords.extend([str(k) for k in kws])
    elif isinstance(categories, list):
        for cat_data in categories:
            if isinstance(cat_data, dict):
                kws = cat_data.get("keywords", [])
                if isinstance(kws, list):
                    keywords.extend([str(k) for k in kws])

    return keywords
