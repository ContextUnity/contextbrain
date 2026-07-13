"""SQLite canonical BrainCell schema migration tests."""

from __future__ import annotations

import sqlite3

from contextunity.brain.storage.sqlite.schema import apply_preflight_renames


def test_preflight_renames_legacy_cell_kind_column_without_data_loss() -> None:
    database = sqlite3.connect(":memory:")
    database.execute(
        "CREATE TABLE cells (id TEXT PRIMARY KEY, tenant_id TEXT, node_kind TEXT, content TEXT)"
    )
    database.execute(
        "INSERT INTO cells (id, tenant_id, node_kind, content) VALUES (?, ?, ?, ?)",
        ("cell-1", "tenant-a", "fact", "bounded fact"),
    )

    apply_preflight_renames(database)

    columns = {row[1] for row in database.execute("PRAGMA table_info(cells)").fetchall()}
    assert "cell_kind" in columns
    assert "node_kind" not in columns
    assert database.execute("SELECT cell_kind FROM cells WHERE id = 'cell-1'").fetchone() == (
        "fact",
    )
