"""Behavioral tests for embedder fail-closed semantics — no sentinel vectors.

A failed or unavailable embedder must raise ``EmbeddingError`` rather than
return a constant ``[0.0]`` / ``[0.1]`` vector that would silently poison the
vector store and mask provider outages. sentence-transformers is absent in CI,
so ``LocalEmbedder`` naturally exercises the "model unavailable" path.
"""

from __future__ import annotations

import httpx
import pytest

from contextunity.brain.core.exceptions import EmbeddingError
from contextunity.brain.service.embedders import ApiEmbedder, EmbeddingCache, LocalEmbedder

pytestmark = pytest.mark.unit


class TestLocalEmbedderFailsClosed:
    """LocalEmbedder raises instead of returning a constant [0.1] vector."""

    @pytest.mark.asyncio
    async def test_embed_async_raises_when_model_unavailable(self) -> None:
        # sentence-transformers is absent → _ensure_model() returns None.
        with pytest.raises(EmbeddingError):
            await LocalEmbedder().embed_async("hello")

    def test_embed_raises_when_model_unavailable(self) -> None:
        with pytest.raises(EmbeddingError):
            LocalEmbedder().embed("hello")


class TestApiEmbedderFailsClosed:
    """ApiEmbedder raises instead of returning a zero vector on API failure."""

    def test_embed_raises_when_called_from_running_loop(self) -> None:
        import asyncio

        embedder = ApiEmbedder(model_name="text-embedding-3-small")

        async def _call_sync_embed() -> None:
            with pytest.raises(RuntimeError, match="embed_async"):
                embedder.embed("hello")

        asyncio.run(_call_sync_embed())

    @pytest.mark.asyncio
    async def test_embed_async_raises_on_transport_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _miss(*_args: object, **_kwargs: object) -> None:
            return None  # force a cache miss so the HTTP path runs

        monkeypatch.setattr(EmbeddingCache, "get", _miss)

        class _FakeClient:
            def __init__(self, **_kwargs: object) -> None:
                pass

            async def __aenter__(self) -> "_FakeClient":
                return self

            async def __aexit__(self, *_exc: object) -> bool:
                return False

            async def post(self, *_args: object, **_kwargs: object) -> object:
                raise httpx.ConnectError("upstream down")

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

        with pytest.raises(EmbeddingError):
            await ApiEmbedder(model_name="text-embedding-3-small").embed_async("some text")
