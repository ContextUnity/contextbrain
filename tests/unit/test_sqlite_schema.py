"""Tests for the local SQLite schema/preflight module.

Zero-infrastructure tests — pure sqlite3 (no sqlite-vec extension required,
since these target the plain-table rename/backfill logic, not vector search).
"""

from __future__ import annotations

import sqlite3

from contextunity.brain.storage.sqlite.schema import apply_preflight_renames, build_core_ddl


class TestEventJournalV0Parity:
    """Event Journal v0 columns (event_id, event_type, severity, status,
    payload, source_refs) must exist on SQLite the same way they exist on
    Postgres — otherwise local dev silently diverges from the production
    schema."""

    _V0_COLUMNS = {"event_id", "event_type", "severity", "status", "payload", "source_refs"}

    def test_fresh_schema_has_v0_columns(self):
        db = sqlite3.connect(":memory:")
        for stmt in build_core_ddl():
            db.execute(stmt)
        columns = {row[1] for row in db.execute("PRAGMA table_info(event_journal)").fetchall()}
        assert self._V0_COLUMNS <= columns

    def test_legacy_db_is_backfilled_with_v0_columns(self):
        """A pre-CP-1 DB has `agent_traces` with none of the v0 columns —
        after the preflight rename, `event_journal` must have all of them."""
        db = sqlite3.connect(":memory:")
        db.execute(
            "CREATE TABLE agent_traces (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL)"
        )
        apply_preflight_renames(db)
        columns = {row[1] for row in db.execute("PRAGMA table_info(event_journal)").fetchall()}
        assert self._V0_COLUMNS <= columns

    def test_backfill_is_idempotent(self):
        db = sqlite3.connect(":memory:")
        for stmt in build_core_ddl():
            db.execute(stmt)
        apply_preflight_renames(db)
        apply_preflight_renames(db)  # must not raise "duplicate column"
        columns = {row[1] for row in db.execute("PRAGMA table_info(event_journal)").fetchall()}
        assert self._V0_COLUMNS <= columns


class TestVecCellsRename:
    """`vec_knowledge_nodes` (sqlite-vec virtual table, pre-CP-1 name) must be
    renamed to `vec_cells` in place, or a developer's existing local vector
    data is silently orphaned once `build_vector_ddl()` creates a new, empty
    `vec_cells` alongside it."""

    def test_legacy_vec_table_is_renamed_and_keeps_data(self):
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE vec_knowledge_nodes (node_id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO vec_knowledge_nodes VALUES ('n1')")
        apply_preflight_renames(db)

        tables = {
            row[0]
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "vec_knowledge_nodes" not in tables
        assert "vec_cells" in tables
        rows = db.execute("SELECT node_id FROM vec_cells").fetchall()
        assert rows == [("n1",)]

    def test_fresh_db_has_no_legacy_vec_table(self):
        db = sqlite3.connect(":memory:")
        apply_preflight_renames(db)  # no-op, nothing to rename
        tables = {
            row[0]
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "vec_knowledge_nodes" not in tables
