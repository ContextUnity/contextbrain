"""SQLite DDL schema management for local Brain backend."""

from __future__ import annotations

import sqlite3

from contextunity.core import get_contextunit_logger

logger = get_contextunit_logger(__name__)

# Current schema version — bump when adding tables/columns.
SCHEMA_VERSION = 4

# CP-1 breaking preflight rename map (legacy -> canonical), mirrors the
# Postgres preflight in `postgres/schema.py`. A developer's persistent
# `~/.contextunity/brain_local.sqlite3` predates this rename, so it must be
# converted in place — otherwise `CREATE TABLE IF NOT EXISTS cells` would
# create an empty canonical table alongside the untouched legacy one.
_TABLE_RENAMES: tuple[tuple[str, str], ...] = (
    ("blackboard_records", "blackboard"),
    ("knowledge_nodes", "cells"),
    ("knowledge_edges", "cell_edges"),
    ("agent_traces", "event_journal"),
    # sqlite-vec virtual table mirroring the `cells` rename above — without
    # this, an existing local DB's vector data stays orphaned in
    # `vec_knowledge_nodes` while `build_vector_ddl()` creates a new, empty
    # `vec_cells` and all search/insert code reads/writes only that one.
    ("vec_knowledge_nodes", "vec_cells"),
)
_COLUMN_RENAMES: tuple[tuple[str, str, str], ...] = (("cells", "taxonomy_path", "scope_path"),)

# Event Journal v0 columns (storage only — no public Event Journal RPCs yet),
# added to Postgres by its preflight migration. Backfilled here the same way
# for an existing local SQLite DB that predates them; the CREATE TABLE below
# already includes them for a fresh DB.
# ``event_id`` has no UNIQUE constraint in SQLite: ``ALTER TABLE ADD COLUMN``
# does not support adding a UNIQUE constraint after table creation (a SQLite
# limitation, not an oversight), and nothing writes/reads this column yet.
_EVENT_JOURNAL_V0_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "TEXT"),
    ("event_type", "TEXT NOT NULL DEFAULT 'trace.logged'"),
    ("severity", "TEXT NOT NULL DEFAULT 'info'"),
    ("status", "TEXT NOT NULL DEFAULT 'recorded'"),
    ("payload", "TEXT NOT NULL DEFAULT '{}'"),
    ("source_refs", "TEXT NOT NULL DEFAULT '[]'"),
)


def apply_preflight_renames(db: sqlite3.Connection) -> None:
    """Rename legacy tables/columns to canonical names, in place.

    Idempotent: only acts on objects that still exist under the legacy
    name, so it is a no-op on a fresh database or one already migrated.
    """
    existing = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    for old, new in _TABLE_RENAMES:
        if old in existing and new not in existing:
            db.execute(f"ALTER TABLE {old} RENAME TO {new}")
            existing.discard(old)
            existing.add(new)

    for table, old_col, new_col in _COLUMN_RENAMES:
        if table not in existing:
            continue
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
        if old_col in columns and new_col not in columns:
            db.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")

    if "event_journal" in existing:
        ej_columns = {row[1] for row in db.execute("PRAGMA table_info(event_journal)").fetchall()}
        for column, ddl in _EVENT_JOURNAL_V0_COLUMNS:
            if column not in ej_columns:
                db.execute(f"ALTER TABLE event_journal ADD COLUMN {column} {ddl}")

    if "cells" in existing:
        cells_columns = {row[1] for row in db.execute("PRAGMA table_info(cells)").fetchall()}
        if "content_hash" not in cells_columns:
            db.execute("ALTER TABLE cells ADD COLUMN content_hash TEXT")


def build_core_ddl() -> list[str]:
    """Core table DDL statements (idempotent)."""
    return [
        # Schema version tracking
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        # Blackboard (Flat Memory) — UUID record API matching Postgres
        """
        CREATE TABLE IF NOT EXISTS blackboard (
            id          TEXT PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            scope_path  TEXT NOT NULL,
            content     TEXT NOT NULL,
            metadata    TEXT,
            ttl_until   TEXT,
            created_by  TEXT,
            created_at  TEXT NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_bb_tenant ON blackboard (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_bb_scope ON blackboard (scope_path)",
        # BrainCells
        """
        CREATE TABLE IF NOT EXISTS cells (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            user_id         TEXT,
            node_kind       TEXT DEFAULT 'concept',
            source_type     TEXT,
            source_id       TEXT,
            title           TEXT,
            content         TEXT NOT NULL,
            struct_data     TEXT,
            keywords_text   TEXT,
            scope_path      TEXT,
            content_hash    TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cells_tenant ON cells (tenant_id)",
        # CellEdges
        """
        CREATE TABLE IF NOT EXISTS cell_edges (
            tenant_id   TEXT NOT NULL,
            source_id   TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            relation    TEXT NOT NULL,
            weight      REAL DEFAULT 1.0,
            metadata    TEXT,
            PRIMARY KEY (tenant_id, source_id, target_id, relation)
        )
        """,
        # Event Journal
        """
        CREATE TABLE IF NOT EXISTS event_journal (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            agent_id        TEXT NOT NULL,
            session_id      TEXT,
            user_id         TEXT,
            graph_name      TEXT,
            tool_calls      TEXT,
            token_usage     TEXT,
            timing_ms       INTEGER,
            security_flags  TEXT,
            metadata        TEXT,
            provenance      TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            event_id        TEXT,
            event_type      TEXT NOT NULL DEFAULT 'trace.logged',
            severity        TEXT NOT NULL DEFAULT 'info',
            status          TEXT NOT NULL DEFAULT 'recorded',
            payload         TEXT NOT NULL DEFAULT '{}',
            source_refs     TEXT NOT NULL DEFAULT '[]'
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ej_tenant ON event_journal (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_ej_session ON event_journal (session_id)",
        # Catalog taxonomy
        """
        CREATE TABLE IF NOT EXISTS catalog_taxonomy (
            tenant_id   TEXT NOT NULL,
            domain      TEXT NOT NULL,
            name        TEXT NOT NULL,
            path        TEXT NOT NULL,
            keywords    TEXT,
            metadata    TEXT,
            updated_at  TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (tenant_id, domain, path)
        )
        """,
        # Episodic events
        """
        CREATE TABLE IF NOT EXISTS episodic_events (
            id          TEXT PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            session_id  TEXT,
            content     TEXT NOT NULL,
            metadata    TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ep_tenant_user ON episodic_events (tenant_id, user_id)",
        # User facts
        """
        CREATE TABLE IF NOT EXISTS user_facts (
            tenant_id   TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            fact_key    TEXT NOT NULL,
            fact_value  TEXT,
            confidence  REAL DEFAULT 1.0,
            source_id   TEXT,
            updated_at  TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (tenant_id, user_id, fact_key)
        )
        """,
        # BrainSynapse (Flat Memory Phase B) — mirrors the canonical Postgres
        # `synapses` table (postgres/schema.py `_experiences_schema`). SQLite
        # has no generated-column support in this SQLite build path, so
        # `q_composite` is stored (not generated) and kept in sync by the
        # storage mixin at write/update time using the same formula.
        """
        CREATE TABLE IF NOT EXISTS synapses (
            id                  TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            agent_id            TEXT NOT NULL,
            graph_name          TEXT,
            graph_run_id        TEXT,
            node_id             TEXT,
            node_name           TEXT,
            action_type         TEXT NOT NULL,
            action_data         TEXT NOT NULL,
            action_data_ref     TEXT,
            context_summary     TEXT,
            thought_trace_ref   TEXT,
            content_hash        TEXT,
            client_id           TEXT,
            node_role           TEXT NOT NULL DEFAULT 'worker',
            fault_class         TEXT,
            status              TEXT NOT NULL DEFAULT 'active',
            q_action            REAL NOT NULL DEFAULT 0.5,
            q_hypothesis        REAL NOT NULL DEFAULT 0.5,
            q_relevance         REAL NOT NULL DEFAULT 0.5,
            q_composite         REAL NOT NULL DEFAULT 0.5,
            scope_path          TEXT,
            metadata            TEXT NOT NULL DEFAULT '{}',
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_synapses_tenant ON synapses (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_agent ON synapses (tenant_id, agent_id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_run ON synapses (graph_run_id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_q_composite ON synapses (q_composite DESC)",
        # Ranked-lookup index: query_synapses scopes by tenant then orders by
        # q_composite DESC — tenant-leading composite avoids scanning other
        # tenants' rows (parity with Postgres synapses_tenant_q_composite_idx).
        "CREATE INDEX IF NOT EXISTS idx_synapses_tenant_q_composite ON synapses (tenant_id, q_composite DESC)",
    ]


def build_vector_ddl(vector_dim: int) -> list[str]:
    """Virtual table DDL for sqlite-vec (requires extension)."""
    return [
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_cells USING vec0(
            node_id TEXT PRIMARY KEY,
            embedding float[{vector_dim}]
        )
        """,
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_episodic_events USING vec0(
            event_id TEXT PRIMARY KEY,
            embedding float[{vector_dim}]
        )
        """,
    ]


__all__ = ["SCHEMA_VERSION", "apply_preflight_renames", "build_core_ddl", "build_vector_ddl"]
