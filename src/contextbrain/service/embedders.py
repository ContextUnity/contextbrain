"""Embedder classes for vector embeddings."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class OpenAIEmbedder:
    """OpenAI API-based embeddings."""

    _instance: Optional["OpenAIEmbedder"] = None

    def __init__(self, model_name: str = "text-embedding-3-small"):
        self._model_name = model_name
        self._api_key = os.getenv("OPENAI_API_KEY")
        self._dim = 1536 if "small" in model_name or "ada" in model_name else 3072
        if not self._api_key:
            logger.warning("OPENAI_API_KEY not set, embeddings will fail")

    @classmethod
    def get_instance(cls) -> "OpenAIEmbedder":
        if cls._instance is None:
            model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
            cls._instance = cls(model_name=model)
        return cls._instance

    def embed(self, text: str) -> list[float]:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.embed_async(text))

    async def embed_async(self, text: str) -> list[float]:
        if not self._api_key:
            logger.error("OPENAI_API_KEY not set")
            return [0.0] * self._dim
        try:
            import httpx

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.openai.com/v1/embeddings",
                    headers={
                        "Authorization": f"Bearer {self._api_key}",
                        "Content-Type": "application/json",
                    },
                    json={"model": self._model_name, "input": text},
                    timeout=30.0,
                )
                response.raise_for_status()
                data = response.json()
                return data["data"][0]["embedding"]
        except Exception as e:
            logger.error(f"OpenAI embedding error: {e}")
            return [0.0] * self._dim


class LocalEmbedder:
    """Local embeddings using SentenceTransformers."""

    _instance: Optional["LocalEmbedder"] = None
    _warned: bool = False

    def __init__(self, model_name: str = "all-mpnet-base-v2"):
        self._model = None
        self._model_name = model_name
        self._dim = 768
        self._fallback_mode = False

    @classmethod
    def get_instance(cls) -> "LocalEmbedder":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_model(self):
        if self._model is not None:
            return self._model
        if self._fallback_mode:
            return None
        try:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self._model_name)
            logger.info(f"Loaded embedding model: {self._model_name}")
        except ImportError:
            self._fallback_mode = True
            logger.critical("SentenceTransformers not installed! Semantic search will NOT work.")
            self._model = None
        return self._model

    def embed(self, text: str) -> list[float]:
        model = self._ensure_model()
        if model is None:
            return [0.1] * self._dim
        import asyncio

        loop = asyncio.get_event_loop()
        vec = loop.run_until_complete(loop.run_in_executor(None, lambda: model.encode([text])[0]))
        return [float(x) for x in vec.tolist()]

    async def embed_async(self, text: str) -> list[float]:
        model = self._ensure_model()
        if model is None:
            return [0.1] * self._dim
        import asyncio

        loop = asyncio.get_event_loop()
        vec = await loop.run_in_executor(None, lambda: model.encode([text])[0])
        return [float(x) for x in vec.tolist()]


def get_embedder():
    """Get embedder based on EMBEDDER_TYPE env var."""
    embedder_type = os.getenv("EMBEDDER_TYPE", "").lower()
    if embedder_type == "openai":
        return OpenAIEmbedder.get_instance()
    elif embedder_type == "local":
        return LocalEmbedder.get_instance()
    else:
        if os.getenv("OPENAI_API_KEY"):
            return OpenAIEmbedder.get_instance()
        return LocalEmbedder.get_instance()


__all__ = ["OpenAIEmbedder", "LocalEmbedder", "get_embedder"]
