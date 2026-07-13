"""Tests for local Brain service factory wiring."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from contextunity.core.exceptions import ConfigurationError

from contextunity.brain.service.local import _validate_local_vector_support


def test_enabled_local_enrichment_rejects_missing_vector_runtime() -> None:
    """Local mode cannot accept embedding work without a vector backend."""
    with pytest.raises(ConfigurationError, match="--extra local-vectors"):
        _validate_local_vector_support(
            enrichment_enabled=True,
            provider="onnx",
            has_sqlite_vec=False,
        )


def test_disabled_local_enrichment_needs_no_vector_dependencies() -> None:
    """Local non-vector workflows stay lightweight while the gate is off."""
    _validate_local_vector_support(
        enrichment_enabled=False,
        provider="onnx",
        has_sqlite_vec=False,
    )


@pytest.mark.asyncio
async def test_local_brain_passes_config_to_permission_interceptor(monkeypatch):
    from contextunity.brain.service import interceptors, local

    cfg = SimpleNamespace(
        shield_url="shield.local:50054",
        port=55051,
        embeddings=SimpleNamespace(dimension=768, provider="onnx"),
        embedding_enrichment=SimpleNamespace(enabled=False),
    )
    seen = {}

    class FakeInterceptor:
        def __init__(self, *, shield_url, config):
            seen["shield_url"] = shield_url
            seen["config"] = config

    class FakeServer:
        def __init__(self, *, interceptors):
            seen["interceptors"] = interceptors

        def add_insecure_port(self, endpoint):
            seen["endpoint"] = endpoint
            return 1

    monkeypatch.setattr("contextunity.brain.core.config.get_core_config", lambda: cfg)
    monkeypatch.setattr(interceptors, "BrainPermissionInterceptor", FakeInterceptor)
    monkeypatch.setattr(
        local.grpc.aio, "server", lambda *, interceptors: FakeServer(interceptors=interceptors)
    )
    monkeypatch.setattr(
        local,
        "SqliteBrainStore",
        lambda *, vector_dim: SimpleNamespace(has_sqlite_vec=lambda: False),
    )
    monkeypatch.setattr(local, "DuckDBStore", lambda: object())
    monkeypatch.setattr(local, "get_embedder", lambda _cfg: object())
    monkeypatch.setattr(local, "BrainService", lambda **kwargs: object())
    monkeypatch.setattr(
        local.brain_pb2_grpc, "add_BrainServiceServicer_to_server", lambda service, server: None
    )

    server = await local.create_local_brain()

    assert isinstance(server.grpc_server, FakeServer)
    assert seen["shield_url"] == "shield.local:50054"
    assert seen["config"] is cfg
    assert seen["endpoint"] == "[::]:55051"
