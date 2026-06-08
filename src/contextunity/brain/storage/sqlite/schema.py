"""SQLite DDL schema management for local Brain backend."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger

logger = get_contextunit_logger(__name__)

# Current schema version — bump when adding tables/columns.
SCHEMA_VERSION = 1


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
        CREATE TABLE IF NOT EXISTS blackboard_records (
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
        "CREATE INDEX IF NOT EXISTS idx_bb_tenant ON blackboard_records (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_bb_scope ON blackboard_records (scope_path)",
        # Knowledge nodes
        """
        CREATE TABLE IF NOT EXISTS knowledge_nodes (
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
            taxonomy_path   TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_kn_tenant ON knowledge_nodes (tenant_id)",
        # Knowledge edges
        """
        CREATE TABLE IF NOT EXISTS knowledge_edges (
            tenant_id   TEXT NOT NULL,
            source_id   TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            relation    TEXT NOT NULL,
            weight      REAL DEFAULT 1.0,
            metadata    TEXT,
            PRIMARY KEY (tenant_id, source_id, target_id, relation)
        )
        """,
        # Agent traces
        """
        CREATE TABLE IF NOT EXISTS agent_traces (
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
            created_at      TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tr_tenant ON agent_traces (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_tr_session ON agent_traces (session_id)",
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
    ]


def build_vector_ddl(vector_dim: int) -> list[str]:
    """Virtual table DDL for sqlite-vec (requires extension)."""
    return [
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_knowledge_nodes USING vec0(
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


__all__ = ["SCHEMA_VERSION", "build_core_ddl", "build_vector_ddl"]
