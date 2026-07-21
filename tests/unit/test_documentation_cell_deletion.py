"""Atomic `_doc` exact-cell deletion storage regressions."""

from __future__ import annotations

from pathlib import Path

import pytest

from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.mark.asyncio
async def test_delete_documentation_cells_is_atomic_and_hash_guarded(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)
    first = await store.upsert_cell(
        tenant_id="_doc",
        cell_id="doc-cell-1",
        cell_kind="documentation",
        content="first chunk",
        content_hash="hash-1",
        source_type="documentation",
        metadata={"source_path": "docs/old.md"},
    )
    second = await store.upsert_cell(
        tenant_id="_doc",
        cell_id="doc-cell-2",
        cell_kind="documentation",
        content="second chunk",
        content_hash="hash-2",
        source_type="documentation",
        metadata={"source_path": "docs/old.md"},
    )

    conflict = await store.delete_documentation_cells(
        tenant_id="_doc",
        targets=[("doc-cell-1", "hash-1"), ("doc-cell-2", "stale-hash")],
    )

    assert conflict == {"status": "conflict", "deleted_count": 0, "expected_count": 2}
    assert await store.get_cell(tenant_id="_doc", cell_id=str(first["id"])) is not None
    assert await store.get_cell(tenant_id="_doc", cell_id=str(second["id"])) is not None

    deleted = await store.delete_documentation_cells(
        tenant_id="_doc",
        targets=[("doc-cell-1", "hash-1"), ("doc-cell-2", "hash-2")],
    )

    assert deleted == {"status": "deleted", "deleted_count": 2, "expected_count": 2}
    assert await store.get_cell(tenant_id="_doc", cell_id=str(first["id"])) is None
    assert await store.get_cell(tenant_id="_doc", cell_id=str(second["id"])) is None


@pytest.mark.asyncio
async def test_delete_documentation_cells_cannot_remove_non_documentation_cell(
    tmp_path: Path,
) -> None:
    store = SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)
    cell = await store.upsert_cell(
        tenant_id="_doc",
        cell_id="fact-cell",
        cell_kind="fact",
        content="must remain",
        content_hash="fact-hash",
        source_type="manual",
    )

    result = await store.delete_documentation_cells(
        tenant_id="_doc",
        targets=[("fact-cell", "fact-hash")],
    )

    assert result == {"status": "conflict", "deleted_count": 0, "expected_count": 1}
    assert await store.get_cell(tenant_id="_doc", cell_id=str(cell["id"])) is not None
