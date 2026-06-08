"""Knowledge graph package.

IMPORTANT: keep imports in this module **lazy**.

The runtime cortex imports `contextunity.brain.ingestion.rag.graph.serialization` to load the
persisted graph. Importing heavy ingestion dependencies (like `networkx`) at package import time
breaks minimal installs (e.g., API-only deployments).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from contextunity.core.narrowing import object_attr

if TYPE_CHECKING:
    from .builder import GraphBuilder
    from .lookup import GraphEnricher
    from .prompts import GRAPH_EXTRACTION_PROMPT

__all__ = ["GRAPH_EXTRACTION_PROMPT", "GraphBuilder", "GraphEnricher"]


def __getattr__(name: str) -> object:
    if name == "GraphBuilder":
        mod = importlib.import_module(".builder", __name__)
        return object_attr(mod, "GraphBuilder")
    if name == "GraphEnricher":
        mod = importlib.import_module(".lookup", __name__)
        return object_attr(mod, "GraphEnricher")
    if name == "GRAPH_EXTRACTION_PROMPT":
        mod = importlib.import_module(".prompts", __name__)
        return object_attr(mod, "GRAPH_EXTRACTION_PROMPT")
    raise AttributeError(name)
