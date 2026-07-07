"""Tests for the `_doc` documentation-ingestion helper: metadata shape,
deterministic id, and idempotent re-ingestion by source_hash.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from contextunity.brain.docs_ingest import DOC_TENANT_ID, content_hash_of, ingest_documentation_cell
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

        results = await store.hybrid_search(
            query_text="BrainSynapse learning trace", query_vec=[], tenant_id=DOC_TENANT_ID
        )

        assert len(results) == 1
        cell = results[0].node
        assert cell.id == doc_id
        assert cell.metadata["doc_type"] == "rpc_reference"
        assert cell.metadata["source_path"] == "packages/core/protos/brain.proto#RecordSynapse"
        assert cell.metadata["phase"] == 2
        assert cell.metadata["visibility"] == "internal"
        assert cell.metadata["source_hash"] == content_hash_of(
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
        results = await store.hybrid_search(
            query_text="Stable content", query_vec=[], tenant_id=DOC_TENANT_ID
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
        results = await store.hybrid_search(
            query_text="updated", query_vec=[], tenant_id=DOC_TENANT_ID
        )
        assert len(results) == 1
        assert results[0].node.content == "Version two, updated."
        assert results[0].node.metadata["source_hash"] == content_hash_of("Version two, updated.")

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

        results = await postgres_store.hybrid_search(
            query_text=f"BrainSynapse learning trace {marker}",
            query_vec=[0.0] * 768,
            tenant_id=DOC_TENANT_ID,
        )
        match = next(r for r in results if r.node.id == doc_id)
        assert match.node.metadata["doc_type"] == "rpc_reference"
        assert match.node.metadata["phase"] == 2
        assert match.node.metadata["visibility"] == "internal"
        assert match.node.metadata["source_hash"] == content_hash_of(content)

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

        results = await postgres_store.hybrid_search(
            query_text=f"Stable content {marker}", query_vec=[0.0] * 768, tenant_id=DOC_TENANT_ID
        )
        matches = [r for r in results if r.node.id == first_id]
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
        # is now a real column under the same (node_kind='chunk', content_hash)
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

        results = await postgres_store.hybrid_search(
            query_text=f"Version two updated {marker}",
            query_vec=[0.0] * 768,
            tenant_id=DOC_TENANT_ID,
        )
        match = next(r for r in results if r.node.id == first_id)
        assert match.node.content == second_content
        assert match.node.metadata["source_hash"] == content_hash_of(second_content)
