"""Tests for local Brain service factory wiring."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_local_brain_passes_config_to_permission_interceptor(monkeypatch):
    from contextunity.brain.service import interceptors, local

    cfg = SimpleNamespace(shield_url="shield.local:50054", port=55051)
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
    monkeypatch.setattr(local, "SqliteVecStorageBackend", lambda: object())
    monkeypatch.setattr(local, "DuckDBStore", lambda: object())
    monkeypatch.setattr(local, "get_embedder", lambda _cfg: object())
    monkeypatch.setattr(local, "BrainService", lambda **kwargs: object())
    monkeypatch.setattr(
        local.brain_pb2_grpc, "add_BrainServiceServicer_to_server", lambda service, server: None
    )

    server = await local.create_local_brain()

    assert isinstance(server, FakeServer)
    assert seen["shield_url"] == "shield.local:50054"
    assert seen["config"] is cfg
    assert seen["endpoint"] == "[::]:55051"
