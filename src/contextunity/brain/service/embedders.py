"""Embedder classes for vector embeddings.

Supports Redis-based embedding cache with automatic fallback
to in-memory LRU cache when Redis is unavailable.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Optional

from contextunity.core import get_contextunit_logger

logger = get_contextunit_logger(__name__)

# Silence noisy httpx logger (logs every OpenAI API call at INFO level)
get_contextunit_logger("httpx").setLevel(logging.WARNING)


# ─── Embedding Cache ─────────────────────────────────────────────────────────


class EmbeddingCache:
    """Two-tier embedding cache: Redis (primary) → in-memory (fallback).

    Redis stores embeddings as JSON strings with TTL.
    In-memory dict acts as fallback when Redis is down or unconfigured.
    """

    PREFIX = "emb:"
    TTL_SECONDS = 86400 * 7  # 7 days
    MEMORY_MAX_SIZE = 2048
    STATS_LOG_INTERVAL = 50  # Log stats every N requests

    def __init__(self, redis_url: Optional[str] = None):
        self._redis = None
        self._redis_url = redis_url
        self._redis_available = False
        self._memory: dict[str, list[float]] = {}
        self._hits = 0
        self._misses = 0

        if redis_url:
            self._init_redis(redis_url)

    def _init_redis(self, url: str) -> None:
        """Try to initialize Redis async client."""
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
            self._redis_available = True
            logger.info("Embedding cache: Redis connected (%s)", url)
        except Exception as e:
            logger.warning("Embedding cache: Redis unavailable (%s), using in-memory fallback", e)
            self._redis_available = False

    @staticmethod
    def make_key(model: str, text: str) -> str:
        """Deterministic cache key from model + text."""
        digest = hashlib.sha256(f"{model}:{text}".encode()).hexdigest()
        return f"{EmbeddingCache.PREFIX}{digest}"

    async def get(self, model: str, text: str) -> Optional[list[float]]:
        """Try to get cached embedding. Returns None on miss."""
        key = self.make_key(model, text)

        # 1. Try Redis
        if self._redis_available:
            try:
                raw = await self._redis.get(key)
                if raw is not None:
                    self._hits += 1
                    return json.loads(raw)
            except Exception:
                # Redis went down — flip to fallback silently
                self._redis_available = False
                logger.warning("Embedding cache: Redis lost, falling back to in-memory")

        # 2. Try in-memory
        if key in self._memory:
            self._hits += 1
            return self._memory[key]

        self._misses += 1
        self._log_stats_periodic()
        return None

    async def put(self, model: str, text: str, embedding: list[float]) -> None:
        """Store embedding in cache (Redis + in-memory)."""
        key = self.make_key(model, text)

        # Store in Redis
        if self._redis_available:
            try:
                await self._redis.set(key, json.dumps(embedding), ex=self.TTL_SECONDS)
            except Exception:
                self._redis_available = False

        # Always store in memory (fast path for hot queries)
        if len(self._memory) >= self.MEMORY_MAX_SIZE:
            oldest = next(iter(self._memory))
            del self._memory[oldest]
        self._memory[key] = embedding

    def _log_stats_periodic(self) -> None:
        """Log cache stats every N requests."""
        total = self._hits + self._misses
        if total > 0 and total % self.STATS_LOG_INTERVAL == 0:
            hit_rate = (self._hits / total) * 100
            logger.info(
                "Embedding cache: %d hits / %d misses (%.0f%% hit rate), memory=%d, redis=%s",
                self._hits,
                self._misses,
                hit_rate,
                len(self._memory),
                "up" if self._redis_available else "down",
            )

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{(self._hits / total * 100):.0f}%" if total else "n/a",
            "memory_size": len(self._memory),
            "redis_available": self._redis_available,
        }


# Module-level singleton
_cache: Optional[EmbeddingCache] = None


def get_embedding_cache(redis_url: Optional[str] = None) -> EmbeddingCache:
    """Get or create the singleton embedding cache."""
    global _cache
    if _cache is None:
        from contextunity.core.config import get_core_config

        url = redis_url or get_core_config().redis_url
        _cache = EmbeddingCache(redis_url=url)
    return _cache


# ─── Embedders ────────────────────────────────────────────────────────────────


class ApiEmbedder:
    """API-based embeddings (OpenAI, Local Ollama/vLLM) with Redis + in-memory cache."""

    _instance: Optional["ApiEmbedder"] = None

    def __init__(self, model_name: str = "text-embedding-3-small"):
        self._model_name = model_name
        from contextunity.brain.core.config.main import Config

        cfg = Config.load()
        self._api_key = cfg.openai.api_key
        # Default to OpenAI, but allow overriding for local API providers (Ollama/vLLM)
        self._base_url = "https://api.openai.com/v1/embeddings"
        if cfg.local.ollama_base_url:
            self._base_url = f"{cfg.local.ollama_base_url}/api/embeddings"
        elif cfg.local.vllm_base_url:
            self._base_url = f"{cfg.local.vllm_base_url}/v1/embeddings"

        self._dim = 1536 if "small" in model_name or "ada" in model_name else 3072
        self._cache = get_embedding_cache()
        if not self._api_key and "openai.com" in self._base_url:
            logger.warning("OPENAI_API_KEY not set for OpenAI API embedder. Embeddings may fail.")

    @classmethod
    def get_instance(cls) -> "ApiEmbedder":
        if cls._instance is None:
            from contextunity.brain.core.config.main import Config

            model = Config.load().openai.embedding_model or "text-embedding-3-small"
            cls._instance = cls(model_name=model)
        return cls._instance

    def embed(self, text: str) -> list[float]:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.embed_async(text))

    async def embed_async(self, text: str) -> list[float]:
        # Check cache
        cached = await self._cache.get(self._model_name, text)
        if cached is not None:
            logger.debug("Embedding cache HIT (stats: %s)", self._cache.stats)
            return cached

        # Cache miss → call API
        try:
            import httpx

            headers = {"Content-Type": "application/json"}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            payload = {"model": self._model_name, "input": text}

            # Adjust payload for Ollama
            if "/api/embeddings" in self._base_url:
                payload = {"model": self._model_name, "prompt": text}

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self._base_url,
                    headers=headers,
                    json=payload,
                    timeout=30.0,
                )
                response.raise_for_status()
                data = response.json()

                if "/api/embeddings" in self._base_url:
                    embedding = data["embedding"]
                else:
                    embedding = data["data"][0]["embedding"]

                # Store in cache
                await self._cache.put(self._model_name, text, embedding)

                return embedding
        except Exception as e:
            logger.error(f"API embedding error: {e}")
            return [0.0] * self._dim


class LocalEmbedder:
    """Local embeddings using SentenceTransformers.

    Default model: 'paraphrase-multilingual-MiniLM-L12-v2' (ideal for bilingual EN/UA setups).
    Runs completely offline on local CPU/GPU.
    """

    _instance: Optional["LocalEmbedder"] = None
    _warned: bool = False

    def __init__(self, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        self._model = None
        self._model_name = model_name
        self._dim = 384  # MiniLM uses 384 dimensions
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
            logger.info(f"Loaded local embedding model: {self._model_name}")
        except ImportError:
            self._fallback_mode = True
            logger.critical(
                "SentenceTransformers not installed! Local semantic search will NOT work."
            )
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


# ─── Factory ──────────────────────────────────────────────────────────────────


def get_embedder(config=None):
    """Get embedder based on config or EMBEDDER_TYPE env var.

    Args:
        config: Optional Brain Config object. When provided, reads
                embedder_type and openai.api_key from it instead of env.
    """
    # Initialize cache with Redis URL from config if available
    redis_url = getattr(config, "redis_url", None) if config else None
    get_embedding_cache(redis_url=redis_url)

    from contextunity.brain.core.config.main import Config

    brain_cfg = config or Config.load()
    embedder_type = brain_cfg.embedder_type.lower()
    openai_key = brain_cfg.openai.api_key

    if embedder_type in ("api", "openai"):
        return ApiEmbedder.get_instance()
    elif embedder_type == "local":
        return LocalEmbedder.get_instance()
    else:
        # Auto-detect fallback
        if openai_key or brain_cfg.local.ollama_base_url or brain_cfg.local.vllm_base_url:
            return ApiEmbedder.get_instance()
        return LocalEmbedder.get_instance()


__all__ = [
    "ApiEmbedder",
    "LocalEmbedder",
    "EmbeddingCache",
    "get_embedder",
    "get_embedding_cache",
]
