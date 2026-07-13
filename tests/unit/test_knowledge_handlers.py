"""Canonical BrainCell handler contract tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from contextunity.core import ContextToken, ContextUnit, contextunit_pb2
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from contextunity.brain.service.handlers.knowledge import KnowledgeHandlersMixin


class _CellStorage:
    async def upsert_cell(self, **_kwargs: object) -> JsonDict:
        return {
            "id": "cell-1",
            "tenant_id": "tenant-a",
            "cell_kind": "fact",
            "source_type": "synthesis",
            "scope_path": "tenant_a.memory",
            "content_hash": "sha256:test",
            "confidence": 0.75,
            "visibility": "tenant",
            "created_at": "2026-07-13T00:00:00Z",
            "updated_at": "2026-07-13T00:00:00Z",
        }


class _KnowledgeService(KnowledgeHandlersMixin):
    def __init__(self) -> None:
        self.storage = _CellStorage()


@pytest.mark.asyncio
async def test_upsert_cell_returns_canonical_storage_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import knowledge

    token = ContextToken(
        token_id="test",
        permissions=(Permissions.BRAIN_WRITE,),
        allowed_tenants=("tenant-a",),
    )
    monkeypatch.setattr(knowledge, "extract_token_from_context", lambda _context: token)
    request = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "cell_kind": "fact",
            "content": "A bounded fact",
            "source_type": "synthesis",
            "scope_path": "tenant_a.memory",
            "content_hash": "sha256:test",
            "confidence": 1.0,
        }
    )

    response = await _KnowledgeService().UpsertCell(
        request.to_protobuf(contextunit_pb2),
        SimpleNamespace(),
    )
    payload = ContextUnit.from_protobuf(response).payload

    assert payload == {
        "id": "cell-1",
        "tenant_id": "tenant-a",
        "cell_kind": "fact",
        "source_type": "synthesis",
        "scope_path": "tenant_a.memory",
        "content_hash": "sha256:test",
        "confidence": 0.75,
        "visibility": "tenant",
        "created_at": "2026-07-13T00:00:00Z",
        "updated_at": "2026-07-13T00:00:00Z",
    }
