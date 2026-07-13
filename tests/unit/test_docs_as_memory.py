"""Docs-as-memory fixture through the canonical documentation BrainCell path."""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from contextunity.core.types import JsonDict

from contextunity.brain.docs_ingest import DOC_TENANT_ID, ingest_documentation_cell
from contextunity.brain.ingest import IngestionService
from contextunity.brain.storage.sqlite import SqliteBrainStore

PASSBYREF_DOC_SECTION = (
    "PassByRef convention: ContextUnit payloads over a size threshold are "
    "written to Blackboard and replaced with a small ref envelope "
    "instead of being duplicated at every graph hop. The envelope carries "
    "memory_ref, ref_kind, content_hash, and expires_at."
)


@pytest.fixture
def store(tmp_path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=str(tmp_path / "docs_test.sqlite3"), vector_dim=8)


async def _ingest_passbyref_doc(
    store: SqliteBrainStore,
    *,
    content: str = PASSBYREF_DOC_SECTION,
    source_path: str = "planner/roadmap/phase-01/phase-01.md#1.5",
    section: str = "1.5",
) -> str:
    return await ingest_documentation_cell(
        store,
        content=content,
        source_path=source_path,
        doc_type="phase_plan",
        phase=1,
        symbol=section,
        visibility="internal",
        metadata={"section": section},
    )


class TestDocsAsMemoryFixture:
    @pytest.mark.asyncio
    async def test_insert_and_query_passbyref_section(self, store: SqliteBrainStore):
        doc_id = await _ingest_passbyref_doc(store)

        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="small ref envelope",
        )

        assert len(results) == 1
        assert results[0]["id"] == doc_id
        assert "memory_ref" in results[0]["content"]
        assert results[0]["metadata"]["doc_type"] == "phase_plan"
        assert results[0]["metadata"]["section"] == "1.5"

    @pytest.mark.asyncio
    async def test_metadata_uses_jsonb_not_dedicated_columns(self, store: SqliteBrainStore):
        await _ingest_passbyref_doc(
            store, content="content", source_path="phase-01.md#1.9", section="1.9"
        )

        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="content",
        )

        assert len(results) == 1
        metadata = results[0]["metadata"]
        assert metadata["doc_type"] == "phase_plan"
        assert metadata["source_path"] == "phase-01.md#1.9"
        assert metadata["section"] == "1.9"
        assert "source_hash" in metadata

    @pytest.mark.asyncio
    async def test_generic_ingestion_writes_canonical_document_cell(
        self, store: SqliteBrainStore, monkeypatch: pytest.MonkeyPatch
    ):
        async def enrich_stub(
            _self: IngestionService, _content: str, metadata: JsonDict
        ) -> JsonDict:
            enriched = dict(metadata)
            enriched["source_ref"] = "docs/generic.md"
            return enriched

        monkeypatch.setattr(IngestionService, "_enrich_metadata", enrich_stub)
        service = IngestionService(store)

        doc_id = await service.ingest_document(
            "Generic docs ingestion writes a BrainCell.",
            {"_doc_id": "generic-doc-cell", "visibility": "internal"},
            tenant_id=DOC_TENANT_ID,
            source_type="documentation",
        )

        assert doc_id == "generic-doc-cell"
        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="document",
            source_type="documentation",
            query_text="BrainCell",
        )
        assert len(results) == 1
        assert results[0]["source_ref"] == "docs/generic.md"


class TestDocTenantIsolation:
    @pytest.mark.asyncio
    async def test_doc_tenant_content_invisible_to_other_tenants(self, store: SqliteBrainStore):
        await _ingest_passbyref_doc(store)

        results = await store.query_cells(
            tenant_id="some-project-tenant",
            cell_kind="documentation",
            source_type="documentation",
            query_text="small ref envelope",
        )

        assert results == []

    @pytest.mark.asyncio
    async def test_other_tenant_content_invisible_from_doc_tenant(self, store: SqliteBrainStore):
        await store.upsert_cell(
            tenant_id="some-project-tenant",
            cell_id="proj-secret",
            cell_kind="documentation",
            content="internal project roadmap details",
            source_type="documentation",
            metadata={"section": "n/a"},
        )

        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="internal project roadmap",
        )

        assert results == []


BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")


@pytest_asyncio.fixture
async def postgres_store():
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — skipping live-Postgres docs-as-memory test")
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    yield store
    await store.close()


class TestDocsAsMemoryPostgresParity:
    @pytest.mark.asyncio
    async def test_insert_and_query_passbyref_section(self, postgres_store):
        marker = uuid.uuid4().hex
        source_path = f"planner/roadmap/phase-01/phase-01-{marker}.md#1.5"
        content = f"{PASSBYREF_DOC_SECTION} (test-marker: {marker})"

        doc_id = await ingest_documentation_cell(
            postgres_store,
            content=content,
            source_path=source_path,
            doc_type="phase_plan",
            phase=1,
            symbol="1.5",
            visibility="internal",
            metadata={"section": "1.5"},
        )

        results = await postgres_store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text=marker,
            limit=10,
        )

        match = next(r for r in results if r["id"] == doc_id)
        assert match["metadata"]["doc_type"] == "phase_plan"
        assert match["metadata"]["section"] == "1.5"
