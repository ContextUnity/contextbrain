"""Base class for ingestion plugins.

This defines the plugin contract for the ingestion pipeline
(`contextunity.brain.ingestion.rag.*`). It is ingestion-specific and intentionally
separate from the framework-level `contextunity.brain.core.interfaces`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable

from contextunity.brain.core import BrainConfig

from ..settings import RagIngestionConfig
from .types import GraphEnrichmentResult, RawData, ShadowRecord


class IngestionPlugin(ABC):
    """Abstract base class for ingestion plugins.

    Each plugin handles a specific content type (video, book, qa, web, knowledge)
    and implements two phases: Load and Transform.

    Plugins can specify a default source directory name, which can be overridden
    in settings.toml via [plugins.{source_type}].dir
    """

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Returns the source type string (e.g., 'video', 'book')."""

    @property
    def default_source_dir(self) -> str:
        """Returns the default source directory name for this plugin.

        Can be overridden in settings.toml via [plugins.{source_type}].dir
        Defaults to source_type if not overridden.

        Returns:
            Default directory name (e.g., 'video', 'q&a')
        """
        return self.source_type

    @abstractmethod
    def load(self, assets_path: str) -> list[RawData]:
        """Scans the assets directory and loads raw content."""

    @abstractmethod
    def transform(
        self,
        data: list[RawData],
        enrichment_func: Callable[[str], GraphEnrichmentResult],
        *,
        taxonomy_path: Path | None = None,
        config: RagIngestionConfig | None = None,
        core_cfg: BrainConfig | None = None,
    ) -> list[ShadowRecord]:
        """Chunks the raw data and transforms it into ShadowRecords."""

    # ---- Optional shared helpers (recommended for consistency across plugins) ----


__all__ = ["IngestionPlugin"]
