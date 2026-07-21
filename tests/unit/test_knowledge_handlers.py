"""Canonical BrainCell handler contract tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from contextunity.core import ContextToken, ContextUnit, contextunit_pb2
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from contextunity.brain.service.handlers.cell_search import CellSearchHandlersMixin
from contextunity.brain.service.handlers.cell_write import CellWriteHandlersMixin
from contextunity.brain.storage.postgres.models import GraphNode, SearchResult


class _CellStorage:
    def vector_backend_available(self) -> bool:
        return False

    async def hybrid_search(self, **kwargs: object) -> list[SearchResult]:
        assert kwargs["tenant_id"] == "tenant-a"
        assert kwargs["source_types"] == ["documentation"]
        assert kwargs["metadata_filter"] == {"service": "contextunity.docs"}
        return [
            SearchResult(
                node=GraphNode(
                    id="cell-search-1",
                    tenant_id="tenant-a",
                    cell_kind="chunk",
                    content="bounded canonical result",
                    source_type="documentation",
                    source_ref="docs/test.md",
                    scope_path="docs.test",
                    content_hash="sha256:result",
                    confidence=0.8,
                    visibility="tenant",
                    metadata={"section": "contract"},
                ),
                score=0.75,
                vector_score=0.7,
                text_score=0.5,
            )
        ]

    async def delete_documentation_cells(self, **kwargs: object) -> JsonDict:
        assert kwargs["tenant_id"] == "_doc"
        assert kwargs["targets"] == [("doc-1", "hash-1")]
        return {"status": "deleted", "deleted_count": 1, "expected_count": 1}

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


class _CellService(CellSearchHandlersMixin, CellWriteHandlersMixin):
    def __init__(self) -> None:
        self.storage = _CellStorage()
        self.embedder = object()


@pytest.mark.asyncio
async def test_search_cells_returns_ranked_canonical_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import cell_search

    token = ContextToken(
        token_id="search-test",
        permissions=(Permissions.BRAIN_READ,),
        allowed_tenants=("tenant-a",),
    )
    monkeypatch.setattr(cell_search, "extract_token_from_context", lambda _context: token)
    request = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "query_text": "canonical result",
            "source_types": ["documentation"],
            "metadata_filter": {"service": "contextunity.docs"},
            "min_score": 0.5,
        }
    ).to_protobuf(contextunit_pb2)

    responses = [
        ContextUnit.from_protobuf(response).payload
        async for response in _CellService().SearchCells(request, SimpleNamespace())
    ]

    assert responses == [
        {
            "id": "cell-search-1",
            "tenant_id": "tenant-a",
            "cell_kind": "chunk",
            "content": "bounded canonical result",
            "score": 0.75,
            "vector_score": 0.7,
            "text_score": 0.5,
            "source_type": "documentation",
            "source_ref": "docs/test.md",
            "scope_path": "docs.test",
            "content_hash": "sha256:result",
            "confidence": 0.8,
            "visibility": "tenant",
            "metadata": {"section": "contract"},
        }
    ]


@pytest.mark.asyncio
async def test_ingest_document_resolves_token_tenant_and_preserves_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain import ingest
    from contextunity.brain.service.handlers import cell_write

    token = ContextToken(
        token_id="ingest-test",
        permissions=(Permissions.BRAIN_WRITE,),
        allowed_tenants=("tenant-a",),
    )
    monkeypatch.setattr(cell_write, "extract_token_from_context", lambda _context: token)
    captured: dict[str, object] = {}

    class _IngestionService:
        def __init__(self, storage: object) -> None:
            captured["storage"] = storage

        async def ingest_document(self, **kwargs: object) -> str:
            captured.update(kwargs)
            return "document-1"

    monkeypatch.setattr(ingest, "IngestionService", _IngestionService)
    request = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "content": "bounded document",
            "source_type": "documentation",
            "metadata": {"source_path": "docs/test.md"},
        }
    ).to_protobuf(contextunit_pb2)

    response = await _CellService().IngestDocument(request, SimpleNamespace())

    assert ContextUnit.from_protobuf(response).payload == {"id": "document-1", "success": True}
    assert captured["tenant_id"] == "tenant-a"
    assert captured["content"] == "bounded document"
    assert captured["source_type"] == "documentation"


@pytest.mark.asyncio
async def test_upsert_cell_returns_canonical_storage_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import cell_write

    token = ContextToken(
        token_id="test",
        permissions=(Permissions.BRAIN_WRITE,),
        allowed_tenants=("tenant-a",),
    )
    monkeypatch.setattr(cell_write, "extract_token_from_context", lambda _context: token)
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

    response = await _CellService().UpsertCell(
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


@pytest.mark.asyncio
async def test_delete_documentation_cells_uses_exact_targets_and_doc_tenant_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import cell_write

    token = ContextToken(
        token_id="test",
        permissions=(Permissions.BRAIN_WRITE, Permissions.DOCS_WRITE),
        allowed_tenants=("_doc",),
    )
    monkeypatch.setattr(cell_write, "extract_token_from_context", lambda _context: token)
    request = ContextUnit(
        payload={
            "tenant_id": "_doc",
            "targets": [{"cell_id": "doc-1", "content_hash": "hash-1"}],
        }
    )

    response = await _CellService().DeleteDocumentationCells(
        request.to_protobuf(contextunit_pb2),
        SimpleNamespace(),
    )

    assert ContextUnit.from_protobuf(response).payload == {
        "status": "deleted",
        "deleted_count": 1,
        "expected_count": 1,
    }
