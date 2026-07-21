"""Tests for the local SQLite schema/preflight module.

Zero-infrastructure tests — pure sqlite3 (no sqlite-vec extension required,
since these target the plain-table rename/backfill logic, not vector search).
"""

from __future__ import annotations

import sqlite3

from contextunity.brain.storage.sqlite.schema import apply_preflight_renames, build_core_ddl


class TestExecutionTraceMigration:
    """SQLite must expose only the canonical trace table after preflight."""

    _TERMINAL_COLUMNS = {
        "graph_run_id",
        "payload_digest",
        "terminal_status",
        "terminal_reason",
        "trace_schema_version",
        "prompt_evidence",
        "steps",
        "control_evidence",
    }

    def test_fresh_schema_has_terminal_columns(self):
        db = sqlite3.connect(":memory:")
        for stmt in build_core_ddl():
            db.execute(stmt)
        columns = {row[1] for row in db.execute("PRAGMA table_info(execution_traces)").fetchall()}
        assert self._TERMINAL_COLUMNS <= columns
        assert (
            db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_journal'"
            ).fetchone()
            is None
        )

    def test_legacy_trace_rows_are_preserved_as_legacy_v0(self):
        db = sqlite3.connect(":memory:")
        db.execute(
            "CREATE TABLE agent_traces (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL)"
        )
        db.execute("INSERT INTO agent_traces VALUES ('trace-1', 'tenant-a', 'agent-a')")
        apply_preflight_renames(db)
        row = db.execute(
            "SELECT id, tenant_id, agent_id, trace_schema_version FROM execution_traces"
        ).fetchone()
        assert row == ("trace-1", "tenant-a", "agent-a", "legacy_v0")

    def test_preflight_is_idempotent(self):
        db = sqlite3.connect(":memory:")
        for stmt in build_core_ddl():
            db.execute(stmt)
        apply_preflight_renames(db)
        apply_preflight_renames(db)
        columns = {row[1] for row in db.execute("PRAGMA table_info(execution_traces)").fetchall()}
        assert self._TERMINAL_COLUMNS <= columns

    def test_fresh_and_migrated_schema_signatures_match(self):
        fresh = sqlite3.connect(":memory:")
        for stmt in build_core_ddl():
            fresh.execute(stmt)

        migrated = sqlite3.connect(":memory:")
        migrated.execute(
            "CREATE TABLE event_journal "
            "(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL)"
        )
        migrated.execute("INSERT INTO event_journal VALUES ('trace-1', 'tenant-a', 'agent-a')")
        apply_preflight_renames(migrated)
        for stmt in build_core_ddl():
            migrated.execute(stmt)

        fresh_columns = fresh.execute("PRAGMA table_info(execution_traces)").fetchall()
        migrated_columns = migrated.execute("PRAGMA table_info(execution_traces)").fetchall()
        assert migrated_columns == fresh_columns
        fresh_indexes = fresh.execute("PRAGMA index_list(execution_traces)").fetchall()
        migrated_indexes = migrated.execute("PRAGMA index_list(execution_traces)").fetchall()
        assert migrated_indexes == fresh_indexes
        assert migrated.execute("SELECT COUNT(*) FROM execution_traces").fetchone() == (1,)

    def test_unknown_generic_event_blocks_and_rolls_back(self):
        db = sqlite3.connect(":memory:")
        db.execute(
            """
            CREATE TABLE event_journal (
                id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL,
                event_type TEXT, severity TEXT, status TEXT, payload TEXT, source_refs TEXT
            )
            """
        )
        db.execute(
            "INSERT INTO event_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("event-1", "tenant-a", "agent-a", "other", "info", "recorded", "{}", "[]"),
        )
        with db:
            try:
                apply_preflight_renames(db)
            except sqlite3.IntegrityError as exc:
                assert "unmapped generic event row" in str(exc)
            else:
                raise AssertionError("unknown generic event must block migration")
        assert db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_journal'"
        ).fetchone() == (1,)


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
