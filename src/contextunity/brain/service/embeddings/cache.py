"""Two-tier cache for provider-generated embedding vectors."""

from __future__ import annotations

import hashlib
import json
from typing import ClassVar, Protocol, runtime_checkable

from contextunity.core import get_contextunit_logger
from contextunity.core.parsing import json_loads
from contextunity.core.types import JsonDict, is_object_list

logger = get_contextunit_logger(__name__)


@runtime_checkable
class _AsyncKeyValueClient(Protocol):
    async def get(self, name: str) -> object: ...

    async def set(self, name: str, value: str, *, ex: int) -> object: ...


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


class EmbeddingCache:
    """Redis cache with an in-process fallback for a single Brain instance."""

    PREFIX: ClassVar[str] = "emb:"
    TTL_SECONDS: ClassVar[int] = 86400 * 7
    MEMORY_MAX_SIZE: ClassVar[int] = 2048
    STATS_LOG_INTERVAL: ClassVar[int] = 50

    def __init__(self, redis_url: str | None = None) -> None:
        self._redis: object | None = None
        self._redis_available = False
        self._memory: dict[str, list[float]] = {}
        self._hits = 0
        self._misses = 0
        if redis_url:
            self._init_redis(redis_url)

    def _init_redis(self, url: str) -> None:
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
            self._redis_available = True
        except Exception:
            logger.warning("Embedding cache: Redis unavailable, using in-memory fallback")

    @staticmethod
    def make_key(model_identity: str, text: str) -> str:
        """Build a cache key that cannot mix different vector spaces."""
        digest = hashlib.sha256(f"{model_identity}:{text}".encode()).hexdigest()
        return f"{EmbeddingCache.PREFIX}{digest}"

    async def get(self, model_identity: str, text: str) -> list[float] | None:
        """Read one cached vector, falling back to the local LRU-like map."""
        key = self.make_key(model_identity, text)
        if self._redis_available:
            try:
                client = self._redis
                if isinstance(client, _AsyncKeyValueClient):
                    fetched = await client.get(key)
                    if isinstance(fetched, str):
                        parsed = _embedding_list_from_json(fetched)
                        if parsed is not None:
                            self._hits += 1
                            return parsed
            except Exception:
                self._redis_available = False
                logger.warning("Embedding cache: Redis lost, using in-memory fallback")
        cached = self._memory.get(key)
        if cached is not None:
            self._hits += 1
            return cached
        self._misses += 1
        self._log_stats_periodic()
        return None

    async def put(self, model_identity: str, text: str, embedding: list[float]) -> None:
        """Store one validated vector in both available cache tiers."""
        key = self.make_key(model_identity, text)
        if self._redis_available:
            try:
                client = self._redis
                if isinstance(client, _AsyncKeyValueClient):
                    _ = await client.set(key, json.dumps(embedding), ex=self.TTL_SECONDS)
            except Exception:
                self._redis_available = False
        if len(self._memory) >= self.MEMORY_MAX_SIZE:
            oldest = next(iter(self._memory))
            del self._memory[oldest]
        self._memory[key] = embedding

    def _log_stats_periodic(self) -> None:
        total = self._hits + self._misses
        if total and total % self.STATS_LOG_INTERVAL == 0:
            logger.info(
                "Embedding cache: %d hits / %d misses, memory=%d, redis=%s",
                self._hits,
                self._misses,
                len(self._memory),
                "up" if self._redis_available else "down",
            )

    @property
    def stats(self) -> JsonDict:
        """Return bounded cache telemetry without vector content."""
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
    """Return the process-local cache initialized from Core Redis settings."""
    global _cache
    if _cache is None:
        from contextunity.core.config import get_core_config

        core = get_core_config()
        url = redis_url or (core.redis.url if core.redis.enabled else None)
        _cache = EmbeddingCache(redis_url=url)
    return _cache


__all__ = ["EmbeddingCache", "get_embedding_cache"]
