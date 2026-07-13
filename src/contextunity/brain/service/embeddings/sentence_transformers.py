"""Optional SentenceTransformers embedding adapter."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from contextunity.core.narrowing import object_attr
from contextunity.core.types import is_object_list

from contextunity.brain.core.config.providers import EmbeddingProviderConfig
from contextunity.brain.core.exceptions import EmbeddingError

from .cache import EmbeddingCache, get_embedding_cache
from .contracts import validate_embedding_vector


@runtime_checkable
class _Vector(Protocol):
    def tolist(self) -> list[float]: ...


@runtime_checkable
class _SentenceTransformerModel(Protocol):
    def encode(self, sentences: list[str]) -> Sequence[_Vector]: ...

    def get_sentence_embedding_dimension(self) -> int: ...


def _load_model(model_name: str) -> _SentenceTransformerModel:
    try:
        module = importlib.import_module("sentence_transformers")
    except ImportError as exc:
        raise EmbeddingError(
            "sentence_transformers embeddings require contextunity-brain[hf-embeddings]"
        ) from exc
    constructor = object_attr(module, "SentenceTransformer")
    if not callable(constructor):
        raise EmbeddingError("sentence_transformers does not expose SentenceTransformer")
    loaded = constructor(model_name)
    if not isinstance(loaded, _SentenceTransformerModel):
        raise EmbeddingError("sentence_transformers model does not satisfy the embedding contract")
    return loaded


class SentenceTransformersEmbedder:
    """Optional local adapter that accepts only a native configured dimension."""

    def __init__(
        self, config: EmbeddingProviderConfig, *, cache: EmbeddingCache | None = None
    ) -> None:
        self._config = config
        self._cache = cache or get_embedding_cache()
        self._model: _SentenceTransformerModel | None = None
        self._load_lock = asyncio.Lock()
        self._identity = f"{config.space_id}:{config.provider}:{config.model}:{config.dimension}"

    def embed(self, text: str) -> list[float]:
        """Synchronously generate one vector."""
        from .contracts import run_coroutine_sync

        return run_coroutine_sync(lambda: self.embed_async(text))

    async def embed_async(self, text: str) -> list[float]:
        """Generate one vector and prove the selected model's native dimension."""
        cached = await self._cache.get(self._identity, text)
        if cached is not None:
            return validate_embedding_vector(cached, config=self._config)
        model = await self._ensure_model()
        vector = await asyncio.to_thread(self._encode_blocking, model, text)
        vector = validate_embedding_vector(vector, config=self._config)
        await self._cache.put(self._identity, text, vector)
        return vector

    async def _ensure_model(self) -> _SentenceTransformerModel:
        if self._model is not None:
            return self._model
        async with self._load_lock:
            if self._model is None:
                loaded = await asyncio.to_thread(_load_model, self._config.model)
                if loaded.get_sentence_embedding_dimension() != self._config.dimension:
                    raise EmbeddingError(
                        "SentenceTransformers model dimension mismatch: "
                        f"model={self._config.model} expected={self._config.dimension} "
                        f"actual={loaded.get_sentence_embedding_dimension()}"
                    )
                self._model = loaded
        return self._model

    @staticmethod
    def _encode_blocking(model: _SentenceTransformerModel, text: str) -> list[float]:
        encoded = model.encode([text])
        if not encoded:
            raise EmbeddingError("SentenceTransformers model returned no vector")
        values = encoded[0].tolist()
        if not is_object_list(values):
            raise EmbeddingError("SentenceTransformers model returned a malformed vector")
        vector: list[float] = []
        for value in values:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise EmbeddingError("SentenceTransformers model returned a malformed vector")
            vector.append(float(value))
        return vector


__all__ = ["SentenceTransformersEmbedder"]
