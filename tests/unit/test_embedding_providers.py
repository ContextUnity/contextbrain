"""Behavioral checks for explicit Brain embedding provider selection."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from contextunity.brain.core.config.main import BrainConfig
from contextunity.brain.core.config.providers import EmbeddingProviderConfig
from contextunity.brain.core.exceptions import EmbeddingError
from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION
from contextunity.brain.service.embeddings import (
    DeterministicEmbedder,
    EmbeddingCache,
    HttpEmbedder,
    OnnxEmbedder,
    get_embedder,
    validate_embedding_vector,
)
from contextunity.brain.service.embeddings import onnx as onnx_module
from contextunity.brain.service.embeddings.onnx import prefetch_onnx_assets
from contextunity.brain.storage.portable import BrainPortableArchiveWriter, PortableManifest
from contextunity.brain.storage.sqlite import SqliteBrainStore

pytestmark = pytest.mark.unit


def test_default_profile_selects_local_onnx_with_matching_storage_dimension(
    tmp_path: Path,
) -> None:
    """A default deployment has one explicit local 768-dimensional vector space."""
    config = BrainConfig()

    assert config.embeddings.provider == "onnx"
    sqlite_store = SqliteBrainStore(db_path=tmp_path / "default-dimension.sqlite3")
    archive_writer = BrainPortableArchiveWriter(tmp_path / "archive")

    assert config.embeddings.dimension == DEFAULT_EMBEDDING_DIMENSION
    assert config.postgres.vector_dim == config.embeddings.dimension
    assert sqlite_store.vector_dim == config.embeddings.dimension
    assert PortableManifest().vector_dim == config.embeddings.dimension
    assert archive_writer.vector_dim == config.embeddings.dimension
    assert isinstance(get_embedder(config), OnnxEmbedder)


def test_configuration_rejects_storage_dimension_mismatch() -> None:
    """DDL and provider configuration cannot silently describe different spaces."""
    with pytest.raises(ValidationError, match="postgres.vector_dim must equal"):
        BrainConfig.model_validate(
            {
                "postgres": {"vector_dim": 384},
                "embeddings": {"dimension": 768},
            }
        )


@pytest.mark.parametrize("provider", ["openai", "ollama", "vllm"])
def test_http_provider_requires_explicit_model_endpoint_and_space(provider: str) -> None:
    """Changing provider cannot inherit ONNX model or guessed endpoint defaults."""
    with pytest.raises(ValidationError):
        EmbeddingProviderConfig.model_validate({"provider": provider})


def test_explicit_http_provider_can_match_a_nondefault_storage_dimension() -> None:
    """An operator-selected remote vector space is valid only with matching DDL."""
    config = BrainConfig.model_validate(
        {
            "postgres": {"vector_dim": 1536},
            "embeddings": {
                "provider": "openai",
                "space_id": "text-embedding-3-small-1536-v1",
                "model": "text-embedding-3-small",
                "dimension": 1536,
                "endpoint": "https://api.openai.com/v1/embeddings",
            },
        }
    )

    assert config.embeddings.model_cache_dir is None


def test_test_provider_is_rejected_outside_debug() -> None:
    """Deterministic vectors cannot be enabled in a normal deployment."""
    with pytest.raises(ValidationError, match="permitted only when brain.debug=true"):
        BrainConfig.model_validate(
            {
                "embeddings": {
                    "provider": "deterministic",
                    "space_id": "test-768-v1",
                    "model": "test",
                }
            }
        )


def test_deterministic_provider_has_configured_native_dimension() -> None:
    """The service-boundary test provider cannot mask a vector-width regression."""
    config = EmbeddingProviderConfig(
        provider="deterministic",
        space_id="test-4-v1",
        model="test",
        dimension=4,
    )

    assert len(DeterministicEmbedder(config).embed("one")) == 4
    with pytest.raises(EmbeddingError, match="dimension mismatch"):
        validate_embedding_vector([0.1], config=config)


def test_onnx_provider_downloads_external_model_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The quantized graph is unusable unless its adjacent external weights exist."""
    downloads: list[tuple[str | None, str, str]] = []

    class FakeHub:
        @staticmethod
        def hf_hub_download(
            *,
            repo_id: str,
            filename: str,
            cache_dir: Path,
            revision: str,
            local_files_only: bool,
            subfolder: str | None = None,
        ) -> str:
            del repo_id, cache_dir
            assert local_files_only is False
            downloads.append((subfolder, filename, revision))
            return f"/cache/{subfolder + '/' if subfolder else ''}{filename}"

    real_import_module = onnx_module.importlib.import_module

    def import_module(name: str) -> object:
        if name == "huggingface_hub":
            return FakeHub()
        return real_import_module(name)

    monkeypatch.setattr(onnx_module.importlib, "import_module", import_module)
    config = EmbeddingProviderConfig(
        provider="onnx",
        space_id="embeddinggemma-768-v1",
        model="onnx-community/embeddinggemma-300m-ONNX",
        dimension=768,
    )

    model_path, tokenizer_path = prefetch_onnx_assets(config)

    assert model_path.endswith("onnx/model_quantized.onnx")
    assert tokenizer_path.endswith("tokenizer.json")
    assert downloads == [
        ("onnx", "model_quantized.onnx", "5090578d9565bb06545b4552f76e6bc2c93e4a66"),
        (
            "onnx",
            "model_quantized.onnx_data",
            "5090578d9565bb06545b4552f76e6bc2c93e4a66",
        ),
        (None, "tokenizer.json", "5090578d9565bb06545b4552f76e6bc2c93e4a66"),
    ]


@pytest.mark.asyncio
async def test_text_fallback_does_not_load_a_provider_without_vector_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PATH L stays usable for text retrieval when sqlite-vec is not installed."""
    from contextunity.brain.service.handlers import knowledge

    class NoVectorStorage:
        def vector_backend_available(self) -> bool:
            return False

    class NeverCalledEmbedder:
        async def embed_async(self, _: str) -> list[float]:
            raise AssertionError("text fallback must not call the embedding provider")

    monkeypatch.setattr(
        knowledge,
        "get_core_config",
        lambda: SimpleNamespace(embeddings=SimpleNamespace(dimension=3)),
    )
    service = SimpleNamespace(storage=NoVectorStorage(), embedder=NeverCalledEmbedder())

    assert await knowledge._query_vector(storage=service, text="documentation") == [0.0, 0.0, 0.0]


class TestHttpProviderFailsClosed:
    """HTTP providers expose transport and output defects as typed failures."""

    def test_embed_raises_when_called_from_running_loop(self) -> None:
        config = EmbeddingProviderConfig(
            provider="openai",
            space_id="remote-3-v1",
            model="text-embedding-3-small",
            dimension=1536,
            endpoint="https://example.invalid/v1/embeddings",
        )
        embedder = HttpEmbedder(config, cache=EmbeddingCache())

        async def call_sync_embed() -> None:
            with pytest.raises(RuntimeError, match="embed_async"):
                embedder.embed("hello")

        asyncio.run(call_sync_embed())

    @pytest.mark.asyncio
    async def test_embed_async_raises_on_transport_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = EmbeddingProviderConfig(
            provider="openai",
            space_id="remote-3-v1",
            model="text-embedding-3-small",
            dimension=1536,
            endpoint="https://example.invalid/v1/embeddings",
        )

        class FakeClient:
            def __init__(self, **_kwargs: object) -> None:
                pass

            async def __aenter__(self) -> "FakeClient":
                return self

            async def __aexit__(self, *_exc: object) -> bool:
                return False

            async def post(self, *_args: object, **_kwargs: object) -> object:
                raise httpx.ConnectError("upstream down")

        monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

        with pytest.raises(EmbeddingError, match="HTTP embedding request failed"):
            await HttpEmbedder(config, cache=EmbeddingCache()).embed_async("some text")
