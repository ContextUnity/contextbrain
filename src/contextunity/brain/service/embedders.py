"""Embedder classes for vector embeddings.

Supports Redis-based embedding cache with automatic fallback
to in-memory LRU cache when Redis is unavailable.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, ClassVar, Protocol, final, runtime_checkable

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_json_dict, object_attr
from contextunity.core.parsing import json_loads
from contextunity.core.types import is_object_iterable, is_object_list

logger = get_contextunit_logger(__name__)

if TYPE_CHECKING:
    from contextunity.brain.core.config.main import BrainConfig

# Silence noisy httpx logger (logs every OpenAI API call at INFO level)
get_contextunit_logger("httpx").setLevel(logging.WARNING)


class _EmbeddingVector(Protocol):
    def tolist(self) -> list[float]: ...


@runtime_checkable
class _AsyncKeyValueClient(Protocol):
    async def get(self, name: str) -> object: ...

    async def set(self, name: str, value: str, *, ex: int) -> object: ...


class _SentenceTransformerModel(Protocol):
    def encode(self, sentences: list[str]) -> Sequence[_EmbeddingVector]: ...


def _embedding_list_from_json(raw: str) -> list[float] | None:
    parsed_obj: object = json_loads(raw)
    if not is_object_list(parsed_obj):
        return None
    values: list[float] = []
    for item_obj in parsed_obj:
        if isinstance(item_obj, bool) or not isinstance(item_obj, (int, float)):
            return None
        values.append(float(item_obj))
    return values


def _embedding_list_from_api(data: object, *, ollama: bool) -> list[float] | None:
    payload = as_json_dict(data)
    raw: object
    if ollama:
        raw = payload.get("embedding")
    else:
        rows_raw: object = payload.get("data")
        if not isinstance(rows_raw, list) or not rows_raw:
            return None
        first_row: object = rows_raw[0]
        first = as_json_dict(first_row)
        raw = first.get("embedding")
    if not is_object_list(raw):
        return None
    values: list[float] = []
    for item_obj in raw:
        if isinstance(item_obj, bool) or not isinstance(item_obj, (int, float)):
            return None
        values.append(float(item_obj))
    return values


@final
class _SentenceTransformerHandle:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def encode(self, sentences: list[str]) -> Sequence[_EmbeddingVector]:
        encode_fn_obj: object = object_attr(self._inner, "encode")
        if not callable(encode_fn_obj):
            raise TypeError("sentence-transformers model missing encode()")
        encode_fn: Callable[[list[str]], object] = encode_fn_obj
        vectors_obj = encode_fn(sentences)
        if not is_object_iterable(vectors_obj):
            raise TypeError("encode() did not return a sequence")
        return [_EmbeddingVectorAdapter(vector) for vector in vectors_obj]


@final
class _EmbeddingVectorAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def tolist(self) -> list[float]:
        tolist_fn_obj: object = object_attr(self._inner, "tolist")
        if not callable(tolist_fn_obj):
            raise TypeError("embedding vector missing tolist()")
        tolist_fn: Callable[[], object] = tolist_fn_obj
        raw_obj = tolist_fn()
        if not is_object_list(raw_obj):
            raise TypeError("tolist() did not return a list")
        values: list[float] = []
        for value in raw_obj:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values.append(float(value))
        return values


def _load_sentence_transformer(model_name: str) -> _SentenceTransformerHandle | None:
    try:
        st_mod = importlib.import_module("sentence_transformers")
    except ImportError:
        return None
    st_ctor_obj: object | None = getattr(st_mod, "SentenceTransformer", None)
    if st_ctor_obj is None or not callable(st_ctor_obj):
        return None
    st_ctor: Callable[[str], object] = st_ctor_obj
    loaded = st_ctor(model_name)
    encode_fn_obj: object | None = getattr(loaded, "encode", None)
    if encode_fn_obj is None or not callable(encode_fn_obj):
        return None
    return _SentenceTransformerHandle(loaded)


def _vector_to_floats(vec: _EmbeddingVector) -> list[float]:
    return [float(x) for x in vec.tolist()]


# ─── Embedding Cache ─────────────────────────────────────────────────────────


class EmbeddingCache:
    """Two-tier embedding cache: Redis (primary) → in-memory (fallback)."""

    PREFIX: ClassVar[str] = "emb:"
    TTL_SECONDS: ClassVar[int] = 86400 * 7
    MEMORY_MAX_SIZE: ClassVar[int] = 2048
    STATS_LOG_INTERVAL: ClassVar[int] = 50

    def __init__(self, redis_url: str | None = None) -> None:
        self._redis: object | None = None
        self._redis_url: str | None = redis_url
        self._redis_available: bool = False
        self._memory: dict[str, list[float]] = {}
        self._hits: int = 0
        self._misses: int = 0

        if redis_url:
            self._init_redis(redis_url)

    def _init_redis(self, url: str) -> None:
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
        digest = hashlib.sha256(f"{model}:{text}".encode()).hexdigest()
        return f"{EmbeddingCache.PREFIX}{digest}"

    async def get(self, model: str, text: str) -> list[float] | None:
        key = self.make_key(model, text)

        if self._redis_available:
            try:
                client = self._redis
                parsed: list[float] | None = None
                if isinstance(client, _AsyncKeyValueClient):
                    fetched_obj = await client.get(key)
                    if isinstance(fetched_obj, str):
                        parsed = _embedding_list_from_json(fetched_obj)
                if parsed is not None:
                    self._hits += 1
                    return parsed
            except Exception:
                self._redis_available = False
                logger.warning("Embedding cache: Redis lost, falling back to in-memory")

        if key in self._memory:
            self._hits += 1
            return self._memory[key]

        self._misses += 1
        self._log_stats_periodic()
        return None

    async def put(self, model: str, text: str, embedding: list[float]) -> None:
        key = self.make_key(model, text)

        if self._redis_available:
            try:
                client = self._redis
                if isinstance(client, _AsyncKeyValueClient):
                    _ = await client.set(
                        key,
                        json.dumps(embedding),
                        ex=self.TTL_SECONDS,
                    )
            except Exception:
                self._redis_available = False

        if len(self._memory) >= self.MEMORY_MAX_SIZE:
            oldest = next(iter(self._memory))
            del self._memory[oldest]
        self._memory[key] = embedding

    def _log_stats_periodic(self) -> None:
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
    def stats(self) -> dict[str, object]:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{(self._hits / total * 100):.0f}%" if total else "n/a",
            "memory_size": len(self._memory),
            "redis_available": self._redis_available,
        }


_cache: EmbeddingCache | None = None


def get_embedding_cache(redis_url: str | None = None) -> EmbeddingCache:
    global _cache
    if _cache is None:
        from contextunity.core.config import get_core_config

        core = get_core_config()
        url = redis_url or (core.redis.url if core.redis.enabled else None)
        _cache = EmbeddingCache(redis_url=url)
    return _cache


# ─── Embedders ────────────────────────────────────────────────────────────────


class ApiEmbedder:
    """API-based embeddings (OpenAI, Local Ollama/vLLM) with Redis + in-memory cache."""

    _instance: ApiEmbedder | None = None

    def __init__(self, model_name: str = "text-embedding-3-small") -> None:
        self._model_name: str = model_name
        from contextunity.brain.core.config import get_core_config

        cfg = get_core_config()
        self._api_key: str | None = cfg.openai.api_key
        self._base_url: str = "https://api.openai.com/v1/embeddings"
        if cfg.local.ollama_base_url:
            self._base_url = f"{cfg.local.ollama_base_url}/api/embeddings"
        elif cfg.local.vllm_base_url:
            self._base_url = f"{cfg.local.vllm_base_url}/v1/embeddings"

        self._dim: int = 1536 if "small" in model_name or "ada" in model_name else 3072
        self._cache: EmbeddingCache = get_embedding_cache()
        if not self._api_key and "openai.com" in self._base_url:
            logger.warning("OPENAI_API_KEY not set for OpenAI API embedder. Embeddings may fail.")

    @classmethod
    def get_instance(cls) -> ApiEmbedder:
        if cls._instance is None:
            from contextunity.brain.core.config import get_core_config

            model = get_core_config().openai.embedding_model or "text-embedding-3-small"
            cls._instance = cls(model_name=model)
        return cls._instance

    def embed(self, text: str) -> list[float]:
        import asyncio

        return asyncio.get_event_loop().run_until_complete(self.embed_async(text))

    async def embed_async(self, text: str) -> list[float]:
        cached = await self._cache.get(self._model_name, text)
        if cached is not None:
            logger.debug("Embedding cache HIT (stats: %s)", self._cache.stats)
            return cached

        try:
            import httpx

            headers = {"Content-Type": "application/json"}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            payload: dict[str, str] = {"model": self._model_name, "input": text}
            ollama = "/api/embeddings" in self._base_url
            if ollama:
                payload = {"model": self._model_name, "prompt": text}

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self._base_url,
                    headers=headers,
                    json=payload,
                    timeout=30.0,
                )
                _ = response.raise_for_status()
                parsed_body: object = json_loads(response.text)
                embedding = _embedding_list_from_api(parsed_body, ollama=ollama)
                if embedding is None:
                    raise ValueError("API response did not contain a valid embedding vector")

                await self._cache.put(self._model_name, text, embedding)
                return embedding
        except Exception as e:
            logger.error("API embedding error: %s", e)
            return [0.0] * self._dim


class LocalEmbedder:
    """Local embeddings using SentenceTransformers."""

    _instance: LocalEmbedder | None = None
    _warned: bool = False

    def __init__(self, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2") -> None:
        self._model: _SentenceTransformerModel | None = None
        self._model_name: str = model_name
        self._dim: int = 384
        self._fallback_mode: bool = False

    @classmethod
    def get_instance(cls) -> LocalEmbedder:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_model(self) -> _SentenceTransformerModel | None:
        if self._model is not None:
            return self._model
        if self._fallback_mode:
            return None
        loaded = _load_sentence_transformer(self._model_name)
        if loaded is None:
            self._fallback_mode = True
            logger.critical(
                "SentenceTransformers not installed! Local semantic search will NOT work."
            )
            self._model = None
        else:
            self._model = loaded
            logger.info("Loaded local embedding model: %s", self._model_name)
        return self._model

    def embed(self, text: str) -> list[float]:
        model = self._ensure_model()
        if model is None:
            return [0.1] * self._dim
        import asyncio

        loop = asyncio.get_event_loop()
        vec = loop.run_until_complete(loop.run_in_executor(None, lambda: model.encode([text])[0]))
        return _vector_to_floats(vec)

    async def embed_async(self, text: str) -> list[float]:
        model = self._ensure_model()
        if model is None:
            return [0.1] * self._dim
        import asyncio

        loop = asyncio.get_event_loop()
        vec = await loop.run_in_executor(None, lambda: model.encode([text])[0])
        return _vector_to_floats(vec)


def get_embedder(config: BrainConfig | None = None) -> ApiEmbedder | LocalEmbedder:
    redis_url = getattr(config, "redis_url", None) if config else None
    _ = get_embedding_cache(redis_url=redis_url)

    from contextunity.brain.core.config import get_core_config

    brain_cfg = config if config is not None else get_core_config()
    embedder_type = brain_cfg.embedder_type.lower()
    openai_key = brain_cfg.openai.api_key

    if embedder_type in ("api", "openai"):
        return ApiEmbedder.get_instance()
    if embedder_type == "local":
        return LocalEmbedder.get_instance()
    if openai_key or brain_cfg.local.ollama_base_url or brain_cfg.local.vllm_base_url:
        return ApiEmbedder.get_instance()
    return LocalEmbedder.get_instance()


__all__ = [
    "ApiEmbedder",
    "EmbeddingCache",
    "LocalEmbedder",
    "get_embedder",
    "get_embedding_cache",
]
