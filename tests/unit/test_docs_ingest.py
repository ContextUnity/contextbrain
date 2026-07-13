"""Tests for the `_doc` documentation-ingestion helper: metadata shape,
deterministic id, and idempotent re-ingestion by source_hash.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from contextunity.core.documentation import DocumentationValidationError

from contextunity.brain.docs_ingest import (
    DOC_TENANT_ID,
    build_test_generated_documentation_cell,
    content_hash_of,
    extract_doc_comment_cells,
    extract_documentation_cells,
    extract_proto_documentation_cells,
    extract_pydantic_config_cells,
    extract_yaml_config_cells,
    ingest_documentation_cell,
    ingest_documentation_sources,
)
from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.fixture
def store(tmp_path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=str(tmp_path / "docs_ingest_test.sqlite3"), vector_dim=8)


class TestIngestDocumentationCell:
    @pytest.mark.asyncio
    async def test_writes_under_doc_tenant_with_full_metadata_shape(self, store: SqliteBrainStore):
        doc_id = await ingest_documentation_cell(
            store,
            content="RecordSynapse writes one BrainSynapse learning trace.",
            source_path="packages/core/protos/brain.proto#RecordSynapse",
            doc_type="rpc_reference",
            phase=2,
            visibility="internal",
        )

        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="BrainSynapse learning trace",
        )

        assert len(results) == 1
        cell = results[0]
        assert cell["id"] == doc_id
        assert cell["metadata"]["doc_type"] == "rpc_reference"
        assert cell["metadata"]["source_path"] == "packages/core/protos/brain.proto#RecordSynapse"
        assert cell["metadata"]["symbol"] == "brain.proto#RecordSynapse"
        assert cell["metadata"]["phase"] == 2
        assert cell["metadata"]["visibility"] == "internal"
        assert cell["metadata"]["lifecycle"] == "draft"
        assert cell["metadata"]["lifecycle_state"] == "draft"
        assert cell["metadata"]["source_hash"] == content_hash_of(
            "RecordSynapse writes one BrainSynapse learning trace."
        )

    @pytest.mark.asyncio
    async def test_reingesting_unchanged_source_is_idempotent(self, store: SqliteBrainStore):
        kwargs = {
            "content": "Stable content.",
            "source_path": "docs/architecture/source_authority.md",
            "doc_type": "architecture",
            "phase": 2,
        }
        first_id = await ingest_documentation_cell(store, **kwargs)
        second_id = await ingest_documentation_cell(store, **kwargs)

        assert first_id == second_id
        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="Stable content",
        )
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_reingesting_changed_content_updates_same_row(self, store: SqliteBrainStore):
        source_path = "docs/runbooks/brain-diagnostics.md"
        first_id = await ingest_documentation_cell(
            store, content="Version one.", source_path=source_path, doc_type="runbook", phase=2
        )
        second_id = await ingest_documentation_cell(
            store,
            content="Version two, updated.",
            source_path=source_path,
            doc_type="runbook",
            phase=2,
        )

        assert first_id == second_id
        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="updated",
        )
        assert len(results) == 1
        assert results[0]["content"] == "Version two, updated."
        assert results[0]["metadata"]["source_hash"] == content_hash_of("Version two, updated.")

    @pytest.mark.asyncio
    async def test_different_source_paths_get_different_ids(self, store: SqliteBrainStore):
        id_a = await ingest_documentation_cell(
            store, content="same text", source_path="a.md", doc_type="architecture", phase=2
        )
        id_b = await ingest_documentation_cell(
            store, content="same text", source_path="b.md", doc_type="architecture", phase=2
        )
        assert id_a != id_b

    @pytest.mark.asyncio
    async def test_deterministic_id_is_stable_across_calls(self, store: SqliteBrainStore, tmp_path):
        """Same source_path always derives the same id, independent of storage state."""
        id_first = await ingest_documentation_cell(
            store, content="v1", source_path="stable/path.md", doc_type="config_ref", phase=2
        )
        other_store = SqliteBrainStore(db_path=str(tmp_path / "other.sqlite3"), vector_dim=8)
        id_second = await ingest_documentation_cell(
            other_store, content="v1", source_path="stable/path.md", doc_type="config_ref", phase=2
        )
        assert id_first == id_second

    @pytest.mark.asyncio
    async def test_ingests_extracted_records_through_same_braincell_api(
        self, store: SqliteBrainStore
    ):
        sources = extract_proto_documentation_cells(
            "packages/core/protos/brain.proto",
            """
            service BrainService {
              rpc UpsertCell (UpsertCellRequest) returns (UpsertCellResponse);
            }
            """,
            phase=3,
        )

        ids = await ingest_documentation_sources(store, sources)

        assert len(ids) == 2
        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            limit=10,
        )
        symbols = {row["metadata"]["symbol"] for row in results}
        assert symbols == {"BrainService", "BrainService.UpsertCell"}

    def test_extracts_proto_service_and_rpc_cells(self):
        cells = extract_proto_documentation_cells(
            "packages/core/protos/brain.proto",
            """
            service BrainService {
              rpc QueryCells (QueryCellsRequest) returns (QueryCellsResponse);
            }
            """,
        )

        assert [cell.symbol for cell in cells] == ["BrainService", "BrainService.QueryCells"]
        assert cells[1].metadata["request_type"] == "QueryCellsRequest"
        assert cells[1].metadata["response_type"] == "QueryCellsResponse"

    def test_extracts_yaml_config_keys(self):
        cells = extract_yaml_config_cells(
            "services/brain/brain.yml",
            """
            memory:
              auto_extract:
                enabled: false
            """,
        )

        assert [cell.symbol for cell in cells] == [
            "memory",
            "memory.auto_extract",
            "memory.auto_extract.enabled",
        ]
        assert cells[-1].metadata["config_key"] == "memory.auto_extract.enabled"

    def test_extracts_doc_comment_cells_and_rejects_invalid_type(self):
        cells = extract_doc_comment_cells(
            "services/brain/example.py",
            "# [DOC:api] GET /cells -- list documentation cells\n",
        )

        assert len(cells) == 1
        assert cells[0].doc_type == "api"
        assert cells[0].content == "GET /cells -- list documentation cells"

        with pytest.raises(DocumentationValidationError):
            extract_doc_comment_cells("bad.py", "# [DOC:not_real] invalid\n")

    def test_extracts_pydantic_config_schema_cells(self):
        cells = extract_pydantic_config_cells(
            "services/brain/src/contextunity/brain/core/config/models.py",
            """
            from pydantic import BaseModel

            class BrainMemoryConfig(BaseModel):
                enabled: bool = False
                max_cells: int = 10
            """,
        )

        assert [cell.symbol for cell in cells] == [
            "BrainMemoryConfig.enabled",
            "BrainMemoryConfig.max_cells",
        ]
        assert cells[0].metadata["annotation"] == "bool"

    def test_dispatches_supported_extractors_for_python_sources(self):
        cells = extract_documentation_cells(
            "services/brain/example.py",
            """
            # [DOC:runbook] Check Brain docs ingestion DLQ.

            class BrainDocsSettings:
                enabled: bool = True
            """,
        )

        assert {cell.symbol for cell in cells} == {"line:2", "BrainDocsSettings.enabled"}

    @pytest.mark.asyncio
    async def test_ingests_test_generated_runbook_through_same_braincell_api(
        self, store: SqliteBrainStore
    ):
        source = build_test_generated_documentation_cell(
            source_path="services/brain/tests/test_docs_ingest.py",
            test_name="test_docs_ingest_runbook",
            content="Runbook: verify documentation ingestion idempotency.",
            doc_type="test_runbook",
        )

        ids = await ingest_documentation_sources(store, [source])

        assert len(ids) == 1
        results = await store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text="idempotency",
            limit=10,
        )
        assert len(results) == 1
        assert results[0]["metadata"]["generated_from"] == "test"
        assert results[0]["metadata"]["doc_type"] == "test_runbook"

    def test_rejects_non_runbook_test_generated_doc_type(self):
        with pytest.raises(DocumentationValidationError):
            build_test_generated_documentation_cell(
                source_path="services/brain/tests/test_docs_ingest.py",
                test_name="bad",
                content="Invalid test-generated API doc.",
                doc_type="api",
            )


BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")


@pytest_asyncio.fixture
async def postgres_store():
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — skipping live-Postgres docs-ingest test")
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    yield store
    await store.close()


class TestIngestDocumentationCellPostgresParity:
    @pytest.mark.asyncio
    async def test_writes_under_doc_tenant_with_full_metadata_shape(self, postgres_store):
        marker = uuid.uuid4().hex
        content = f"RecordSynapse writes one BrainSynapse learning trace ({marker})."
        doc_id = await ingest_documentation_cell(
            postgres_store,
            content=content,
            source_path=f"packages/core/protos/brain.proto#RecordSynapse-{marker}",
            doc_type="rpc_reference",
            phase=2,
            visibility="internal",
        )

        # Documentation cells are cell_kind='documentation'; use query_cells
        # (same contract as SQLite parity), not hybrid_search (chunk-only).
        results = await postgres_store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text=f"BrainSynapse learning trace {marker}",
        )
        match = next(r for r in results if r["id"] == doc_id)
        assert match["metadata"]["doc_type"] == "rpc_reference"
        assert match["metadata"]["phase"] == 2
        assert match["metadata"]["visibility"] == "internal"
        assert match["metadata"]["source_hash"] == content_hash_of(content)

    @pytest.mark.asyncio
    async def test_reingesting_unchanged_source_is_idempotent(self, postgres_store):
        marker = uuid.uuid4().hex
        kwargs = {
            "content": f"Stable content ({marker}).",
            "source_path": f"docs/architecture/source_authority-{marker}.md",
            "doc_type": "architecture",
            "phase": 2,
        }
        first_id = await ingest_documentation_cell(postgres_store, **kwargs)
        second_id = await ingest_documentation_cell(postgres_store, **kwargs)
        assert first_id == second_id

        results = await postgres_store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text=f"Stable content {marker}",
        )
        matches = [r for r in results if r["id"] == first_id]
        assert len(matches) == 1

    @pytest.mark.asyncio
    async def test_different_source_paths_get_different_ids(self, postgres_store):
        marker = uuid.uuid4().hex
        id_a = await ingest_documentation_cell(
            postgres_store,
            content=f"same text ({marker})",
            source_path=f"a-{marker}.md",
            doc_type="architecture",
            phase=2,
        )
        id_b = await ingest_documentation_cell(
            postgres_store,
            content=f"same text ({marker})",
            source_path=f"b-{marker}.md",
            doc_type="architecture",
            phase=2,
        )
        assert id_a != id_b

    @pytest.mark.asyncio
    async def test_deterministic_id_is_stable_across_calls(self, postgres_store):
        """Same source_path always derives the same id, independent of storage
        state — same proof as SQLite's version, computed twice against the
        same live-Postgres store since the id derivation is pure and doesn't
        depend on storage backend at all."""
        marker = uuid.uuid4().hex
        kwargs = {
            "content": f"v1 ({marker})",
            "source_path": f"stable/path-{marker}.md",
            "doc_type": "config_ref",
            "phase": 2,
        }
        id_first = await ingest_documentation_cell(postgres_store, **kwargs)
        id_second = await ingest_documentation_cell(postgres_store, **kwargs)
        assert id_first == id_second

    @pytest.mark.asyncio
    async def test_reingesting_changed_content_updates_same_row(self, postgres_store):
        # Unique per-run marker in both source_path AND content: content_hash
        # is now a real column under the same (cell_kind='chunk', content_hash)
        # unique index the RAG chunk-upload path already relies on, so two
        # runs re-inserting the literal same content text (even under
        # different deterministic ids) would collide on that constraint.
        marker = uuid.uuid4().hex
        source_path = f"docs/parity-check-{marker}.md"
        second_content = f"Version two, updated ({marker})."
        first_id = await ingest_documentation_cell(
            postgres_store,
            content=f"Version one ({marker}).",
            source_path=source_path,
            doc_type="runbook",
            phase=2,
        )
        second_id = await ingest_documentation_cell(
            postgres_store,
            content=second_content,
            source_path=source_path,
            doc_type="runbook",
            phase=2,
        )
        assert first_id == second_id

        results = await postgres_store.query_cells(
            tenant_id=DOC_TENANT_ID,
            cell_kind="documentation",
            source_type="documentation",
            query_text=f"Version two updated {marker}",
        )
        match = next(r for r in results if r["id"] == first_id)
        assert match["content"] == second_content
        assert match["metadata"]["source_hash"] == content_hash_of(second_content)
