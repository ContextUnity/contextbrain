"""Test-only deterministic embedding provider."""

from __future__ import annotations

import hashlib
import math

from contextunity.brain.core.config.providers import EmbeddingProviderConfig

from .contracts import validate_embedding_vector


class DeterministicEmbedder:
    """Generate stable non-semantic vectors for service-boundary tests only."""

    def __init__(self, config: EmbeddingProviderConfig) -> None:
        self._config = config

    def embed(self, text: str) -> list[float]:
        """Generate one stable vector without model or network dependencies."""
        values: list[float] = []
        counter = 0
        while len(values) < self._config.dimension:
            digest = hashlib.sha256(f"{counter}:{text}".encode()).digest()
            values.extend((byte / 127.5) - 1.0 for byte in digest)
            counter += 1
        vector = values[: self._config.dimension]
        norm = math.sqrt(sum(value * value for value in vector))
        normalized = [value / norm for value in vector]
        return validate_embedding_vector(normalized, config=self._config)

    async def embed_async(self, text: str) -> list[float]:
        """Asynchronously expose the same deterministic provider contract."""
        return self.embed(text)

    async def embed_query_async(self, text: str) -> list[float]:
        """Expose the deterministic vector through the query role."""
        return self.embed(text)

    async def embed_document_async(self, text: str) -> list[float]:
        """Expose the deterministic vector through the document role."""
        return self.embed(text)


__all__ = ["DeterministicEmbedder"]
