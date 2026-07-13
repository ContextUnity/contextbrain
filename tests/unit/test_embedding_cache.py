"""Tests for EmbeddingCache two-tier cache (Redis → in-memory fallback).

Uses _FakeRedis to test Redis path without a live server, same pattern
as Router's PersistenceMixin tests.
"""

from __future__ import annotations

import json

import pytest

from contextunity.brain.service.embeddings import EmbeddingCache

# ── Fake Redis ────────────────────────────────────────────────────


class _FakeRedis:
    """Minimal async Redis stub for cache testing."""

    def __init__(self):
        self.data: dict[str, str] = {}
        self.set_calls: list[tuple[str, str]] = []

    async def get(self, key: str):
        return self.data.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.data[key] = value
        self.set_calls.append((key, value))


class _BrokenRedis:
    """Redis stub that always raises on operations."""

    async def get(self, key: str):
        raise ConnectionError("Redis down")

    async def set(self, key: str, value: str, ex: int | None = None):
        raise ConnectionError("Redis down")


# ═══════════════════════════════════════════════════════════════════
# make_key
# ═══════════════════════════════════════════════════════════════════


class TestMakeKey:
    """Deterministic cache key generation."""

    def test_deterministic(self):
        k1 = EmbeddingCache.make_key("model-a", "hello")
        k2 = EmbeddingCache.make_key("model-a", "hello")
        assert k1 == k2

    def test_different_model_different_key(self):
        k1 = EmbeddingCache.make_key("model-a", "hello")
        k2 = EmbeddingCache.make_key("model-b", "hello")
        assert k1 != k2

    def test_different_text_different_key(self):
        k1 = EmbeddingCache.make_key("model-a", "hello")
        k2 = EmbeddingCache.make_key("model-a", "world")
        assert k1 != k2

    def test_has_prefix(self):
        key = EmbeddingCache.make_key("m", "t")
        assert key.startswith("emb:")


# ═══════════════════════════════════════════════════════════════════
# In-memory only (no Redis)
# ═══════════════════════════════════════════════════════════════════


class TestMemoryOnly:
    """Cache with no Redis configured."""

    @pytest.mark.asyncio
    async def test_miss_returns_none(self):
        cache = EmbeddingCache()
        result = await cache.get("model", "text")
        assert result is None

    @pytest.mark.asyncio
    async def test_put_then_get(self):
        cache = EmbeddingCache()
        embedding = [0.1, 0.2, 0.3]
        await cache.put("model", "hello", embedding)
        result = await cache.get("model", "hello")
        assert result == embedding

    @pytest.mark.asyncio
    async def test_hit_counter(self):
        cache = EmbeddingCache()
        await cache.put("m", "t", [1.0])
        await cache.get("m", "t")  # hit
        assert cache._hits == 1

    @pytest.mark.asyncio
    async def test_miss_counter(self):
        cache = EmbeddingCache()
        await cache.get("m", "missing")  # miss
        assert cache._misses == 1

    @pytest.mark.asyncio
    async def test_memory_eviction(self):
        """Memory cache evicts oldest when reaching MEMORY_MAX_SIZE."""
        cache = EmbeddingCache()
        cache.MEMORY_MAX_SIZE = 3

        await cache.put("m", "a", [1.0])
        await cache.put("m", "b", [2.0])
        await cache.put("m", "c", [3.0])
        await cache.put("m", "d", [4.0])  # should evict "a"

        assert len(cache._memory) == 3
        # "a" was evicted (FIFO), "d" is newest
        assert await cache.get("m", "d") == [4.0]

    @pytest.mark.asyncio
    async def test_stats_property(self):
        cache = EmbeddingCache()
        await cache.put("m", "t", [1.0])
        await cache.get("m", "t")  # hit
        await cache.get("m", "miss")  # miss
        stats = cache.stats
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["memory_size"] == 1
        assert stats["redis_available"] is False


# ═══════════════════════════════════════════════════════════════════
# With _FakeRedis
# ═══════════════════════════════════════════════════════════════════


class TestWithFakeRedis:
    """Cache with injected fake Redis."""

    def _cache_with_redis(self, redis=None) -> EmbeddingCache:
        cache = EmbeddingCache()
        cache._redis = redis or _FakeRedis()
        cache._redis_available = True
        return cache

    @pytest.mark.asyncio
    async def test_put_stores_in_redis(self):
        fake = _FakeRedis()
        cache = self._cache_with_redis(fake)
        await cache.put("m", "hello", [0.5, 0.6])
        key = EmbeddingCache.make_key("m", "hello")
        assert key in fake.data
        assert json.loads(fake.data[key]) == [0.5, 0.6]

    @pytest.mark.asyncio
    async def test_get_from_redis(self):
        fake = _FakeRedis()
        key = EmbeddingCache.make_key("m", "hello")
        fake.data[key] = json.dumps([0.1, 0.2])
        cache = self._cache_with_redis(fake)
        result = await cache.get("m", "hello")
        assert result == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_also_stores_in_memory(self):
        """Put stores in BOTH Redis and memory."""
        cache = self._cache_with_redis()
        await cache.put("m", "t", [1.0])
        key = EmbeddingCache.make_key("m", "t")
        assert key in cache._memory


# ═══════════════════════════════════════════════════════════════════
# Fallback when Redis fails
# ═══════════════════════════════════════════════════════════════════


class TestRedisFallback:
    """Cache degrades gracefully when Redis goes down."""

    @pytest.mark.asyncio
    async def test_get_falls_back_on_redis_error(self):
        cache = EmbeddingCache()
        cache._redis = _BrokenRedis()
        cache._redis_available = True

        # Pre-populate memory
        key = EmbeddingCache.make_key("m", "t")
        cache._memory[key] = [0.42]

        result = await cache.get("m", "t")
        assert result == [0.42]
        assert cache._redis_available is False  # flipped to fallback

    @pytest.mark.asyncio
    async def test_put_continues_on_redis_error(self):
        cache = EmbeddingCache()
        cache._redis = _BrokenRedis()
        cache._redis_available = True

        await cache.put("m", "t", [0.99])
        # Should have stored in memory despite Redis failure
        key = EmbeddingCache.make_key("m", "t")
        assert cache._memory[key] == [0.99]
        assert cache._redis_available is False
