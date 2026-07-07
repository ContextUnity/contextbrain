"""Module providing Module docstring is missing capabilities."""

from __future__ import annotations

from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_json_dict, as_str, as_str_list
from contextunity.core.parsing import yaml_load
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.storage.contracts import BrainStorageProtocol

logger = get_contextunit_logger(__name__)


class TaxonomyManager:
    """Manages project-specific taxonomies and provides matching logic."""

    project_path: Path
    storage: BrainStorageProtocol | None
    metadata_path: Path
    categories: JsonDict
    sizes: JsonDict
    colors: JsonDict
    pending_verifications: list[JsonDict]

    def __init__(self, project_path: str, storage: BrainStorageProtocol | None = None) -> None:
        self.project_path = Path(project_path)
        self.storage = storage
        self.metadata_path = self.project_path / "metadata"
        categories_yaml = self._load_yaml("categories.yaml")
        taxonomy = as_json_dict(categories_yaml.get("taxonomy"))
        self.categories = as_json_dict(taxonomy.get("categories"))
        self.sizes = as_json_dict(self._load_yaml("sizes.yaml").get("size_taxonomy"))
        self.colors = as_json_dict(self._load_yaml("colors.yaml").get("color_taxonomy"))
        self.pending_verifications = []

    async def sync_to_db(self, tenant_id: str = "default") -> None:
        """Upload YAML taxonomy to Postgres catalog_taxonomy table."""
        if not self.storage:
            logger.warning("No storage provider configured for Taxonomy sync.")
            return

        logger.info("Syncing taxonomy to DB for tenant %s...", tenant_id)

    def _load_yaml(self, filename: str) -> JsonDict:
        path = self.metadata_path / filename
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as handle:
            return as_json_dict(yaml_load(handle))

    def match_category(self, text: str) -> str | None:
        text_lower = text.lower()

        def _search(nodes: object, prefix: str = "") -> str | None:
            if not is_json_dict(nodes):
                return None
            for key, node_raw in nodes.items():
                node = as_json_dict(node_raw)
                keywords = as_str_list(node.get("keywords"))
                if any(kw.lower() in text_lower for kw in keywords):
                    children = as_json_dict(node.get("children"))
                    child_match = _search(children, prefix=f"{prefix}{key}.")
                    return child_match if child_match else f"{prefix}{key}"
            return None

        return _search(self.categories)

    def resolve_size(self, size_text: str, category_context: str = "") -> JsonDict:
        size_upper = size_text.strip().upper()
        groups = as_json_dict(self.sizes.get("groups"))

        if "sleeping_bags" in category_context:
            sleeping_bags = as_json_dict(groups.get("sleeping_bags"))
            overrides = as_json_dict(sleeping_bags.get("overrides"))
            override_entry = as_json_dict(overrides.get(size_upper))
            meaning = as_str(override_entry.get("meaning"))
            if meaning:
                return {
                    "resolved": meaning,
                    "standard": "sleeping_bag_spec",
                }

        return {"resolved": size_text, "standard": "alpha"}

    def get_pending_for_ui(self) -> list[JsonDict]:
        return self.pending_verifications


__all__ = ["TaxonomyManager"]
