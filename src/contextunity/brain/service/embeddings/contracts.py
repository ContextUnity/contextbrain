"""Shared contracts and validation for Brain embedding providers."""

from __future__ import annotations

import math
from collections.abc import Callable, Coroutine
from typing import Protocol, TypeVar

from contextunity.brain.core.config.providers import EmbeddingProviderConfig
from contextunity.brain.core.exceptions import EmbeddingError


class Embedder(Protocol):
    """Generate vectors in one configured, deployment-wide vector space."""

    async def embed_async(self, text: str) -> list[float]:
        """Return one validated vector for ``text``."""
        ...

    def embed(self, text: str) -> list[float]:
        """Synchronously return one validated vector for ``text``."""
        ...


_T = TypeVar("_T")


def run_coroutine_sync(factory: Callable[[], Coroutine[object, object, _T]]) -> _T:
    """Run an async provider call only outside an active event loop."""
    import asyncio

    try:
        _ = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    raise RuntimeError("embed() cannot be called from an async context; use embed_async() instead")


def validate_embedding_vector(
    vector: list[float], *, config: EmbeddingProviderConfig
) -> list[float]:
    """Reject malformed vectors before they can reach vector storage."""
    if len(vector) != config.dimension:
        raise EmbeddingError(
            "Embedding dimension mismatch: "
            f"provider={config.provider} model={config.model} "
            f"expected={config.dimension} actual={len(vector)}"
        )
    if not all(math.isfinite(value) for value in vector):
        raise EmbeddingError("Embedding vector contains non-finite values")
    return vector


__all__ = ["Embedder", "run_coroutine_sync", "validate_embedding_vector"]
