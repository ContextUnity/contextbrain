"""Docs-as-memory fixture: documentation fragments are inserted and searched
through the canonical `cells` path — the same substrate later dashboard/admin
tooling hardens for browsing.

Uses the real PassByRef section text (see
`packages/core/src/contextunity/core/passbyref.py`) as the fixture content,
so this doubles as a real, findable documentation record rather than
throwaway lorem ipsum.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from contextunity.brain.storage.postgres.models import GraphNode
from contextunity.brain.storage.sqlite import SqliteBrainStore

# `_doc` — the canonical documentation tenant. It is an ordinary tenant_id
# string as far as RLS/storage is concerned — no special-casing exists
# anywhere in schema.py or the RLS policies, which is exactly what makes it
# safe: it gets the same isolation guarantees as any project tenant, proven
# generically by every cross-tenant test in this suite. `TestDocTenantIsolation`
# below makes that explicit for `_doc` specifically rather than leaving it implicit.
DOC_TENANT = "_doc"

PASSBYREF_DOC_SECTION = (
    "PassByRef convention: ContextUnit payloads over a size threshold are "
    "written to Blackboard and replaced with a small reference envelope "
    "instead of being duplicated at every graph hop. The envelope carries "
    "memory_ref, ref_kind, content_hash, and expires_at."
)


@pytest.fixture
def store(tmp_path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=str(tmp_path / "docs_test.sqlite3"), vector_dim=8)


def _doc_cell(cell_id: str, content: str, *, section: str) -> GraphNode:
    return GraphNode(
        id=cell_id,
        content=content,
        node_kind="chunk",
        source_type="documentation",
        metadata={
            "doc_type": "phase-01-fixture",
            "source_file": "planner/roadmap/phase-01/phase-01.md",
            "section": section,
        },
    )


class TestDocsAsMemoryFixture:
    @pytest.mark.asyncio
    async def test_insert_and_search_passbyref_section(self, store: SqliteBrainStore):
        await store.upsert_graph(
            [_doc_cell("doc-passbyref-1-5", PASSBYREF_DOC_SECTION, section="1.5")],
            [],
            tenant_id=DOC_TENANT,
        )

        results = await store.hybrid_search(
            query_text="small reference envelope",
            query_vec=[],
            tenant_id=DOC_TENANT,
        )

        assert len(results) == 1
        assert results[0].node.id == "doc-passbyref-1-5"
        assert "memory_ref" in results[0].node.content
        assert results[0].node.metadata["doc_type"] == "phase-01-fixture"
        assert results[0].node.metadata["section"] == "1.5"

    @pytest.mark.asyncio
    async def test_metadata_uses_jsonb_not_dedicated_columns(self, store: SqliteBrainStore):
        """Phase/section tracking must live in `metadata`, never a dedicated
        column — the same convention already used by RecordSynapsePayload
        and other extensible-metadata payloads in this codebase."""
        await store.upsert_graph(
            [_doc_cell("doc-meta-check", "content", section="1.9")], [], tenant_id=DOC_TENANT
        )

        results = await store.hybrid_search(
            query_text="content", query_vec=[], tenant_id=DOC_TENANT
        )

        assert len(results) == 1
        assert results[0].node.metadata == {
            "doc_type": "phase-01-fixture",
            "source_file": "planner/roadmap/phase-01/phase-01.md",
            "section": "1.9",
        }


class TestDocTenantIsolation:
    """`_doc` gets no special treatment — proven explicitly, not just assumed
    from the generic cross-tenant tests elsewhere in this suite."""

    @pytest.mark.asyncio
    async def test_doc_tenant_content_invisible_to_other_tenants(self, store: SqliteBrainStore):
        await store.upsert_graph(
            [_doc_cell("doc-isolated", PASSBYREF_DOC_SECTION, section="1.5")],
            [],
            tenant_id=DOC_TENANT,
        )

        results = await store.hybrid_search(
            query_text="small reference envelope",
            query_vec=[],
            tenant_id="some-project-tenant",
        )

        assert results == []

    @pytest.mark.asyncio
    async def test_other_tenant_content_invisible_from_doc_tenant(self, store: SqliteBrainStore):
        await store.upsert_graph(
            [_doc_cell("proj-secret", "internal project roadmap details", section="n/a")],
            [],
            tenant_id="some-project-tenant",
        )

        results = await store.hybrid_search(
            query_text="internal project roadmap", query_vec=[], tenant_id=DOC_TENANT
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
    async def test_insert_and_search_passbyref_section(self, postgres_store):
        """This runs against a shared, persistent live Postgres instance with
        no per-test-file reset, so the `_doc` tenant accumulates rows across
        runs over time. A unique marker in both the inserted content and the
        search query keeps this row the only possible match regardless of how
        much duplicate content exists — a fixed ``limit`` alone would not,
        since tied ``ts_rank_cd`` scores aren't guaranteed to favor the
        newest row.
        """
        import uuid

        marker = uuid.uuid4().hex
        cell_id = f"doc-passbyref-{marker}"
        content = f"{PASSBYREF_DOC_SECTION} (test-marker: {marker})"
        await postgres_store.upsert_graph(
            [_doc_cell(cell_id, content, section="1.5")], [], tenant_id=DOC_TENANT
        )

        results = await postgres_store.hybrid_search(
            query_text=f"PassByRef reference envelope {marker}",
            query_vec=[0.0] * 768,
            tenant_id=DOC_TENANT,
        )

        assert any(r.node.id == cell_id for r in results)
        match = next(r for r in results if r.node.id == cell_id)
        assert match.node.metadata["doc_type"] == "phase-01-fixture"
