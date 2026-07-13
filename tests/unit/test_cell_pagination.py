"""Canonical BrainCell pagination and filter-order tests."""

from __future__ import annotations

import pytest

from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.mark.asyncio
async def test_sqlite_filters_before_applying_cell_page(tmp_path) -> None:
    store = SqliteBrainStore(db_path=str(tmp_path / "cells.sqlite3"), vector_dim=8)
    try:
        await store.upsert_cell(
            tenant_id="tenant-a",
            cell_kind="documentation",
            content="new unrelated document",
            metadata={"source_path": "other.md"},
            source_type="documentation",
        )
        await store.upsert_cell(
            tenant_id="tenant-a",
            cell_kind="documentation",
            content="first target chunk",
            metadata={"source_path": "target.md"},
            source_type="documentation",
        )
        await store.upsert_cell(
            tenant_id="tenant-a",
            cell_kind="documentation",
            content="second target chunk",
            metadata={"source_path": "target.md"},
            source_type="documentation",
        )

        first = await store.query_cells(
            tenant_id="tenant-a",
            metadata_filter={"source_path": "target.md"},
            limit=1,
            offset=0,
        )
        second = await store.query_cells(
            tenant_id="tenant-a",
            metadata_filter={"source_path": "target.md"},
            limit=1,
            offset=1,
        )

        assert len(first) == len(second) == 1
        assert first[0]["id"] != second[0]["id"]
        assert first[0]["metadata"]["source_path"] == "target.md"
        assert second[0]["metadata"]["source_path"] == "target.md"
    finally:
        await store.close()
