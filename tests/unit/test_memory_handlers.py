"""Memory handler contract tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from contextunity.core import ContextToken, ContextUnit, contextunit_pb2
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from contextunity.brain.service.handlers.memory import MemoryHandlersMixin


class FakeEpisodeStorage:
    """Storage fake for memory handler stream tests."""

    def __init__(self) -> None:
        self.calls: list[JsonDict] = []

    async def get_old_episodes(
        self, *, tenant_id: str, older_than_days: int, limit: int
    ) -> list[JsonDict]:
        self.calls.append(
            {"tenant_id": tenant_id, "older_than_days": older_than_days, "limit": limit}
        )
        return [
            {
                "id": "ep-old",
                "user_id": "user-1",
                "content": "old episode",
                "metadata": {
                    "source_hash": "sha256:test",
                    "graph_run_id": "graph-run-1",
                    "synapse_ids": ["syn-1"],
                },
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]


class MemoryServiceForTest(MemoryHandlersMixin):
    """Concrete memory-handler service with injected storage fake."""

    def __init__(self, storage: FakeEpisodeStorage) -> None:
        self.storage = storage


def _token() -> ContextToken:
    return ContextToken(
        token_id="test",
        permissions=(Permissions.MEMORY_READ,),
        allowed_tenants=("tenant-a",),
    )


@pytest.mark.asyncio
async def test_get_old_episodes_streams_storage_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """GetOldEpisodes exercises the real ContextUnit handler contract."""
    from contextunity.brain.service.handlers import memory as memory_handler

    storage = FakeEpisodeStorage()
    service = MemoryServiceForTest(storage)
    monkeypatch.setattr(memory_handler, "extract_token_from_context", lambda _context: _token())
    unit = ContextUnit(
        payload={"tenant_id": "tenant-a", "older_than_days": 30, "limit": 5},
        provenance=["test:get_old_episodes"],
    )

    rows = [
        ContextUnit.from_protobuf(response).payload
        async for response in service.GetOldEpisodes(
            unit.to_protobuf(contextunit_pb2),
            SimpleNamespace(),
        )
    ]

    assert storage.calls == [{"tenant_id": "tenant-a", "older_than_days": 30, "limit": 5}]
    assert rows == [
        {
            "id": "ep-old",
            "user_id": "user-1",
            "content": "old episode",
            "metadata": {
                "source_hash": "sha256:test",
                "graph_run_id": "graph-run-1",
                "synapse_ids": ["syn-1"],
            },
            "created_at": "2026-01-01T00:00:00Z",
            "source_hash": "sha256:test",
            "graph_run_id": "graph-run-1",
        }
    ]
