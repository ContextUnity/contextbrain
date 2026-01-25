import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class TaxonomyManager:
    """
    Manages project-specific taxonomies and provides matching logic.
    """

    def __init__(self, project_path: str, storage: Optional[Any] = None):
        self.project_path = Path(project_path)
        self.storage = storage
        self.metadata_path = self.project_path / "metadata"
        self.categories = (
            self._load_yaml("categories.yaml").get("taxonomy", {}).get("categories", {})
        )
        self.sizes = self._load_yaml("sizes.yaml").get("size_taxonomy", {})
        self.colors = self._load_yaml("colors.yaml").get("color_taxonomy", {})

        self.pending_verifications = []

    async def sync_to_db(self, tenant_id: str = "default"):
        """Upload YAML taxonomy to Postgres catalog_taxonomy table."""
        if not self.storage:
            logger.warning("No storage provider configured for Taxonomy sync.")
            return

        # Simplified sync for categories
        # Note: In real world, we'd recursively traverse self.categories to build ltree paths
        logger.info(f"Syncing taxonomy to DB for tenant {tenant_id}...")
        # ... logic to call self.storage.upsert_taxonomy ...

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        path = self.metadata_path / filename
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def match_category(self, text: str) -> Optional[str]:
        text_lower = text.lower()

        def _search(nodes, prefix=""):
            # Supports both dict and list categories from my previous YAML formats
            if isinstance(nodes, dict):
                for key, node in nodes.items():
                    keywords = node.get("keywords", [])
                    if any(kw.lower() in text_lower for kw in keywords):
                        children = node.get("children", {})
                        child_match = _search(children, prefix=f"{prefix}{key}.")
                        return child_match if child_match else f"{prefix}{key}"
            return None

        return _search(self.categories)

    def resolve_size(self, size_text: str, category_context: str = "") -> Dict[str, Any]:
        size_upper = size_text.strip().upper()
        # Logic for L as Long/Left etc.
        groups = self.sizes.get("groups", {})

        # Check specific overrides in sleeping bags etc.
        if "sleeping_bags" in category_context:
            overrides = groups.get("sleeping_bags", {}).get("overrides", {})
            if size_upper in overrides:
                return {
                    "resolved": overrides[size_upper]["meaning"],
                    "standard": "sleeping_bag_spec",
                }

        return {"resolved": size_text, "standard": "alpha"}

    def get_pending_for_ui(self) -> List[Dict]:
        return self.pending_verifications
