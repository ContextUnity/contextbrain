"""Postgres schema DDL for knowledge store (pgvector + ltree).

Modules:
- core: BrainCells, CellEdges, cell aliases, episodes, user_facts (always included)
- commerce: Dealer products, taxonomy (for e-commerce)
- news_engine: Raw news, facts, posts (for news pipeline)

Canonical naming: tables and columns use the canonical Flat Memory nouns
(``cells``, ``cell_edges``, ``cell_aliases``, ``scope_path``, ``synapses``,
``event_journal``, ``blackboard``). Legacy physical names (``knowledge_nodes``,
``knowledge_edges``, ``knowledge_aliases``, ``taxonomy_path``,
``agent_experiences``, ``agent_traces``, ``blackboard_records``) appear only in
``_preflight_rename_sql()`` below, which converts an older database to
canonical names in place before the canonical DDL below is applied.
"""

from __future__ import annotations

from collections.abc import Sequence

from contextunity.brain.core.exceptions import BrainValidationError


def _extension_statements() -> list[str]:
    """Extensions required by Brain — need superuser privileges.

    These are separated from table DDL because they may require
    elevated privileges. If they fail, ensure_schema logs a clear
    message instead of aborting all table creation.
    """
    return [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "CREATE EXTENSION IF NOT EXISTS ltree;",
    ]


def _rename_table(old: str, new: str) -> str:
    """Idempotent table rename: no-op on a fresh DB or an already-migrated one."""
    return f"ALTER TABLE IF EXISTS {old} RENAME TO {new};"


def _rename_index(old: str, new: str) -> str:
    """Idempotent index/PK-backing-index rename (Postgres supports IF EXISTS here)."""
    return f"ALTER INDEX IF EXISTS {old} RENAME TO {new};"


def _rename_constraint(table: str, old: str, new: str) -> str:
    """Idempotent CHECK/FK constraint rename.

    ``ALTER TABLE ... RENAME CONSTRAINT`` has no ``IF EXISTS`` clause in
    PostgreSQL, so existence is guarded explicitly — safe on a fresh DB
    (guard is false), a legacy DB (guard is true, rename applied), and an
    already-migrated DB (old name is gone, guard is false). The lookup is
    scoped to the table and current schema: a bare ``conname`` match could
    hit a same-named constraint on another table/schema, making the guard
    pass while the ALTER fails. Keep textually identical to
    ``_rename_constraint()`` in ``migrations/versions/0009_cp1_storage_reset.py``
    (``test_schema_ddl.py::TestMigrationPreflightParity`` enforces this).
    """
    return f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname = '{old}'
                  AND t.relname = '{table}'
                  AND n.nspname = current_schema()
            ) THEN
                ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new};
            END IF;
        END
        $$;
    """


def _rename_column(table: str, old: str, new: str) -> str:
    """Idempotent column rename, guarded the same way as constraint renames."""
    return f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = '{table}' AND column_name = '{old}'
            ) THEN
                ALTER TABLE {table} RENAME COLUMN {old} TO {new};
            END IF;
        END
        $$;
    """


def _drop_legacy_policy(table: str, old_policy: str) -> str:
    """Drop an RLS policy created under a table's legacy (pre-rename) name.

    Policies stay attached to a table across ``RENAME TO`` (they're bound by
    OID, not name), so after step 1 renames e.g. ``knowledge_nodes`` to
    ``cells``, the old ``knowledge_nodes_tenant_isolation`` policy is still
    present on ``cells`` alongside the canonical ``cells_tenant_isolation``
    one ``_rls_policies()`` creates. Both enforce identical logic (harmless
    for RLS semantics — PostgreSQL ORs permissive policies) but the
    duplicate breaks physical-name parity between a migrated and a fresh
    database. ``DROP POLICY IF EXISTS ... ON <table>`` is safe even when
    ``<table>`` itself does not exist yet (fresh DB, preflight runs before
    ``CREATE TABLE``): PostgreSQL emits a NOTICE and continues.
    """
    return f"DROP POLICY IF EXISTS {old_policy} ON {table};"


def _preflight_rename_sql() -> list[str]:
    """Breaking preflight: rename legacy physical names to canonical names.

    Must run before ``build_schema_sql()``'s ``CREATE TABLE IF NOT EXISTS``
    statements — otherwise a legacy database would end up with both the old
    table (still holding data) and a new, empty canonically-named table.

    Every statement here is idempotent by construction:
      - fresh DB: nothing to rename, all guards are false, no-op.
      - legacy DB: first run performs the rename.
      - already-migrated DB: old names are gone, all guards are false, no-op.

    No statement is wrapped in try/except — a failure here must fail the
    startup (fail closed) rather than silently leave a half-migrated schema,
    per the recorded breaking-preflight policy (`00_OVERVIEW.md`, 2026-06-12).
    """
    stmts: list[str] = []

    # 1. Tables (order-independent: Postgres tracks FKs by OID, not name).
    stmts.append(_rename_table("knowledge_nodes", "cells"))
    stmts.append(_rename_table("knowledge_edges", "cell_edges"))
    stmts.append(_rename_table("knowledge_aliases", "cell_aliases"))
    stmts.append(_rename_table("blackboard_records", "blackboard"))
    stmts.append(_rename_table("agent_experiences", "synapses"))
    stmts.append(_rename_table("agent_traces", "event_journal"))

    # 2. Columns (table must already be renamed by step 1).
    stmts.append(_rename_column("cells", "taxonomy_path", "scope_path"))

    # 3. PK-backing and secondary indexes.
    stmts.append(_rename_index("knowledge_nodes_pkey", "cells_pkey"))
    stmts.append(_rename_index("knowledge_nodes_taxonomy_path_gist", "cells_scope_path_gist"))
    stmts.append(_rename_index("knowledge_nodes_embedding_hnsw", "cells_embedding_hnsw"))
    stmts.append(_rename_index("knowledge_nodes_search_vector_gin", "cells_search_vector_gin"))
    stmts.append(_rename_index("knowledge_nodes_keywords_vector_gin", "cells_keywords_vector_gin"))
    stmts.append(_rename_index("knowledge_nodes_source_type_idx", "cells_source_type_idx"))
    stmts.append(_rename_index("knowledge_nodes_source_id_idx", "cells_source_id_idx"))
    stmts.append(_rename_index("knowledge_nodes_node_kind_idx", "cells_node_kind_idx"))
    stmts.append(_rename_index("knowledge_nodes_tenant_idx", "cells_tenant_idx"))
    stmts.append(_rename_index("knowledge_nodes_struct_data_gin", "cells_struct_data_gin"))
    stmts.append(
        _rename_index("knowledge_nodes_chunk_content_hash_uq", "cells_chunk_content_hash_uq")
    )

    stmts.append(_rename_index("knowledge_edges_pkey", "cell_edges_pkey"))
    stmts.append(_rename_index("knowledge_edges_source_idx", "cell_edges_source_idx"))
    stmts.append(_rename_index("knowledge_edges_target_idx", "cell_edges_target_idx"))
    stmts.append(_rename_index("knowledge_edges_relation_idx", "cell_edges_relation_idx"))
    stmts.append(_rename_index("knowledge_edges_tenant_idx", "cell_edges_tenant_idx"))

    stmts.append(_rename_index("knowledge_aliases_pkey", "cell_aliases_pkey"))
    stmts.append(_rename_index("knowledge_aliases_node_id_idx", "cell_aliases_node_id_idx"))
    stmts.append(_rename_index("knowledge_aliases_tenant_idx", "cell_aliases_tenant_idx"))

    stmts.append(_rename_index("blackboard_records_pkey", "blackboard_pkey"))

    stmts.append(_rename_index("agent_experiences_pkey", "synapses_pkey"))
    stmts.append(_rename_index("experiences_q_composite_idx", "synapses_q_composite_idx"))
    stmts.append(_rename_index("experiences_scope_gist", "synapses_scope_gist"))
    stmts.append(_rename_index("experiences_agent_idx", "synapses_agent_idx"))
    stmts.append(_rename_index("experiences_run_idx", "synapses_run_idx"))
    stmts.append(_rename_index("experiences_status_idx", "synapses_status_idx"))
    stmts.append(_rename_index("experiences_tenant_idx", "synapses_tenant_idx"))

    stmts.append(_rename_index("agent_traces_pkey", "event_journal_pkey"))
    stmts.append(_rename_index("agent_traces_tenant_idx", "event_journal_tenant_idx"))
    stmts.append(_rename_index("agent_traces_agent_idx", "event_journal_agent_idx"))
    stmts.append(_rename_index("agent_traces_session_idx", "event_journal_session_idx"))
    stmts.append(_rename_index("agent_traces_created_idx", "event_journal_created_idx"))
    stmts.append(
        _rename_index("agent_traces_tenant_created_idx", "event_journal_tenant_created_idx")
    )

    # 4. CHECK/FK constraints (verified against a live legacy-upgraded database, 2026-07-04).
    stmts.append(
        _rename_constraint("cells", "knowledge_nodes_node_kind_check", "cells_node_kind_check")
    )
    stmts.append(
        _rename_constraint("cells", "knowledge_nodes_source_type_check", "cells_source_type_check")
    )
    stmts.append(
        _rename_constraint(
            "cell_edges", "knowledge_edges_source_id_fkey", "cell_edges_source_id_fkey"
        )
    )
    stmts.append(
        _rename_constraint(
            "cell_edges", "knowledge_edges_target_id_fkey", "cell_edges_target_id_fkey"
        )
    )
    stmts.append(
        _rename_constraint(
            "cell_aliases", "knowledge_aliases_node_id_fkey", "cell_aliases_node_id_fkey"
        )
    )
    stmts.append(
        _rename_constraint(
            "synapses", "agent_experiences_node_role_check", "synapses_node_role_check"
        )
    )
    stmts.append(
        _rename_constraint(
            "synapses", "agent_experiences_fault_class_check", "synapses_fault_class_check"
        )
    )
    stmts.append(
        _rename_constraint("synapses", "agent_experiences_status_check", "synapses_status_check")
    )

    # 5. RLS policies (verified against a live legacy-upgraded database,
    # 2026-07-04): renaming a table does not rename policies attached to
    # it, so the legacy policy names would otherwise linger forever.
    stmts.append(_drop_legacy_policy("cells", "knowledge_nodes_tenant_isolation"))
    stmts.append(_drop_legacy_policy("cell_edges", "knowledge_edges_tenant_isolation"))
    stmts.append(_drop_legacy_policy("cell_aliases", "knowledge_aliases_tenant_isolation"))
    stmts.append(_drop_legacy_policy("blackboard", "blackboard_records_tenant_isolation"))
    stmts.append(_drop_legacy_policy("synapses", "agent_experiences_tenant_isolation"))
    stmts.append(_drop_legacy_policy("event_journal", "agent_traces_tenant_isolation"))

    return stmts


def _core_schema(vector_dim: int) -> list[str]:
    """Core Brain tables - always required."""
    return [
        # BrainCells table
        """
        CREATE TABLE IF NOT EXISTS cells (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            user_id         TEXT NULL,
            node_kind       TEXT NOT NULL CHECK (node_kind IN ('chunk', 'concept')),

            source_type     TEXT NULL CHECK (source_type IN ('video','book','qa','web','knowledge','documentation','product','dealer_product')),
            source_id       TEXT NULL,
            title           TEXT NULL,
            content         TEXT NOT NULL,
            struct_data     JSONB NOT NULL DEFAULT '{}'::jsonb,
            keywords_text   TEXT NULL,

            content_hash    TEXT NULL,
            scope_path      LTREE NULL,

            search_vector   TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
            keywords_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', COALESCE(keywords_text, ''))) STORED,
            embedding       VECTOR(%d) NULL,

            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        # Indexes
        """
        CREATE INDEX IF NOT EXISTS cells_scope_path_gist
          ON cells USING GIST (scope_path);
        """,
        """
        CREATE INDEX IF NOT EXISTS cells_embedding_hnsw
          ON cells USING hnsw (embedding vector_cosine_ops);
        """,
        """
        CREATE INDEX IF NOT EXISTS cells_search_vector_gin
          ON cells USING GIN (search_vector);
        """,
        """
        CREATE INDEX IF NOT EXISTS cells_keywords_vector_gin
          ON cells USING GIN (keywords_vector);
        """,
        "CREATE INDEX IF NOT EXISTS cells_source_type_idx ON cells (source_type);",
        "CREATE INDEX IF NOT EXISTS cells_source_id_idx ON cells (source_id);",
        "CREATE INDEX IF NOT EXISTS cells_node_kind_idx ON cells (node_kind);",
        "CREATE INDEX IF NOT EXISTS cells_tenant_idx ON cells (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS cells_struct_data_gin
          ON cells USING GIN (struct_data);
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS cells_chunk_content_hash_uq
          ON cells (node_kind, content_hash)
          WHERE node_kind = 'chunk' AND content_hash IS NOT NULL;
        """,
        # CellEdges table
        """
        CREATE TABLE IF NOT EXISTS cell_edges (
            tenant_id   TEXT NOT NULL,
            source_id   TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
            target_id   TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
            relation    TEXT NOT NULL,
            weight      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (tenant_id, source_id, target_id, relation)
        );
        """,
        "CREATE INDEX IF NOT EXISTS cell_edges_source_idx ON cell_edges (source_id);",
        "CREATE INDEX IF NOT EXISTS cell_edges_target_idx ON cell_edges (target_id);",
        "CREATE INDEX IF NOT EXISTS cell_edges_relation_idx ON cell_edges (relation);",
        "CREATE INDEX IF NOT EXISTS cell_edges_tenant_idx ON cell_edges (tenant_id);",
        # Cell aliases table
        """
        CREATE TABLE IF NOT EXISTS cell_aliases (
            tenant_id   TEXT NOT NULL,
            alias       TEXT NOT NULL,
            node_id     TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
            source      TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, alias)
        );
        """,
        "CREATE INDEX IF NOT EXISTS cell_aliases_node_id_idx ON cell_aliases (node_id);",
        "CREATE INDEX IF NOT EXISTS cell_aliases_tenant_idx ON cell_aliases (tenant_id);",
        # Episodic Memory (The Journal)
        """
        CREATE TABLE IF NOT EXISTS episodic_events (
            id          UUID PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            session_id  TEXT NULL,
            content     TEXT NOT NULL,
            embedding   VECTOR(%d) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS episodic_events_user_idx ON episodic_events (user_id);",
        "CREATE INDEX IF NOT EXISTS episodic_events_session_idx ON episodic_events (session_id);",
        "CREATE INDEX IF NOT EXISTS episodic_events_tenant_idx ON episodic_events (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS episodic_events_embedding_hnsw
          ON episodic_events USING hnsw (embedding vector_cosine_ops);
        """,
        # Entity Memory (User Facts / Profile)
        """
        CREATE TABLE IF NOT EXISTS user_facts (
            tenant_id   TEXT NOT NULL DEFAULT 'default',
            user_id     TEXT NOT NULL,
            fact_key    TEXT NOT NULL,
            fact_value  JSONB NOT NULL,
            confidence  DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            source_id   UUID NULL REFERENCES episodic_events(id) ON DELETE SET NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, user_id, fact_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS user_facts_user_idx ON user_facts (user_id);",
        "CREATE INDEX IF NOT EXISTS user_facts_tenant_idx ON user_facts (tenant_id);",
        # Event Journal (Observability) — v0: trace-compatible append-only event storage.
        """
        CREATE TABLE IF NOT EXISTS event_journal (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       TEXT NOT NULL,
            agent_id        TEXT NOT NULL,
            session_id      TEXT NULL,
            user_id         TEXT NULL,
            graph_name      TEXT NULL,
            tool_calls      JSONB NOT NULL DEFAULT '[]'::jsonb,
            token_usage     JSONB NOT NULL DEFAULT '{}'::jsonb,
            timing_ms       INTEGER NULL,
            security_flags  JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            provenance      TEXT[] NULL,
            event_id        UUID UNIQUE DEFAULT gen_random_uuid(),
            event_type      TEXT NOT NULL DEFAULT 'trace.logged',
            severity        TEXT NOT NULL DEFAULT 'info',
            status          TEXT NOT NULL DEFAULT 'recorded',
            payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
            source_refs     JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS event_journal_tenant_idx ON event_journal (tenant_id);",
        "CREATE INDEX IF NOT EXISTS event_journal_agent_idx ON event_journal (agent_id);",
        "CREATE INDEX IF NOT EXISTS event_journal_session_idx ON event_journal (session_id);",
        "CREATE INDEX IF NOT EXISTS event_journal_created_idx ON event_journal (created_at DESC);",
        "CREATE INDEX IF NOT EXISTS event_journal_tenant_created_idx ON event_journal (tenant_id, created_at DESC);",
    ]


def _commerce_schema(vector_dim: int) -> list[str]:
    """Commerce/Taxonomy tables - for e-commerce integrations."""
    return [
        # Catalog Taxonomy (The Gold Standard)
        # Note: Support both singular and plural domain names for compatibility
        """
        CREATE TABLE IF NOT EXISTS catalog_taxonomy (
            tenant_id   TEXT NOT NULL,
            domain      TEXT NOT NULL CHECK (domain IN (
                'category', 'categories',
                'color', 'colors',
                'size', 'sizes',
                'gender', 'genders'
            )),
            name        TEXT NOT NULL,
            path        LTREE NOT NULL,
            keywords    TEXT[] NOT NULL DEFAULT '{}',
            embedding   VECTOR(%d) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, domain, path)
        );
        """
        % int(vector_dim),
        "CREATE INDEX IF NOT EXISTS catalog_taxonomy_path_gist ON catalog_taxonomy USING GIST (path);",
        "CREATE INDEX IF NOT EXISTS catalog_taxonomy_domain_idx ON catalog_taxonomy (domain);",
        """
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_embedding_hnsw
          ON catalog_taxonomy USING hnsw (embedding vector_cosine_ops);
        """,
    ]


def _column_backfill() -> list[str]:
    """Idempotent column additions for tables that already exist.

    Problem: ``CREATE TABLE IF NOT EXISTS`` skips the entire statement when
    the table exists, so columns added in later code versions never appear.

    Solution: each entry here is an ``ALTER TABLE … ADD COLUMN IF NOT EXISTS``
    that runs on every startup. PostgreSQL executes it as a no-op when the
    column is already present.

    When you add a new column to a CREATE TABLE, also add the matching
    ALTER TABLE here so existing deployments pick it up automatically.
    """
    return [
        # event_journal — provenance tracking (added after initial trace system)
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS provenance TEXT[] NULL;",
        # event_journal — Event Journal v0 columns; a table that was just
        # renamed from agent_traces predates these and needs the backfill.
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS event_id UUID DEFAULT gen_random_uuid();",
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'trace.logged';",
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'info';",
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'recorded';",
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS source_refs JSONB NOT NULL DEFAULT '[]'::jsonb;",
        # user_facts — tenant isolation (changed PK from (user_id, fact_key)
        # to (tenant_id, user_id, fact_key); the column default handles existing rows)
        "ALTER TABLE user_facts ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';",
        # synapses — canonical BrainSynapse contract fields
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS action_data_ref TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS thought_trace_ref TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS content_hash TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS node_id TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS node_name TEXT NULL;",
    ]


def _constraint_upgrades() -> list[str]:
    """Idempotent constraint updates for evolved schemas.

    CHECK constraints and other DDL that may need updating on existing tables.
    Each statement is wrapped to be safe on re-run.
    """
    return [
        # cells — added 'documentation' to source_type enum
        # DROP + re-ADD is idempotent:  IF EXISTS prevents error on first run
        "ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_source_type_check;",
        """ALTER TABLE cells ADD CONSTRAINT cells_source_type_check
           CHECK (source_type IN ('video','book','qa','web','knowledge','documentation','product','dealer_product'));""",
        # event_journal.event_id must be unique for external references (Event Journal v0)
        "ALTER TABLE event_journal DROP CONSTRAINT IF EXISTS event_journal_event_id_key;",
        "ALTER TABLE event_journal ADD CONSTRAINT event_journal_event_id_key UNIQUE (event_id);",
        # synapses — fault taxonomy adds 'policy_fault' and 'reference_fault'
        # alongside the original 'agent_fault' / 'infra_fault' / 'upstream_fault' set.
        "ALTER TABLE synapses DROP CONSTRAINT IF EXISTS synapses_fault_class_check;",
        """ALTER TABLE synapses ADD CONSTRAINT synapses_fault_class_check
           CHECK (fault_class IS NULL OR fault_class IN
               ('agent_fault','infra_fault','upstream_fault','policy_fault','reference_fault'));""",
    ]


def _blackboard_schema(vector_dim: int) -> list[str]:
    """Blackboard scratch data table — Flat Memory Phase A.

    UNLOGGED table: no WAL overhead (2-3x faster writes). Acceptable because
    blackboard is ephemeral scratch data — loss on PostgreSQL crash is OK,
    the graph will re-execute. Trade-off: not replicated to standby.
    """
    return [
        """
        CREATE UNLOGGED TABLE IF NOT EXISTS blackboard (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id   TEXT NOT NULL,
            scope_path  LTREE NOT NULL,
            content     JSONB NOT NULL,
            embedding   VECTOR(%d) NULL,
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
            ttl_until   TIMESTAMPTZ NULL,
            created_by  TEXT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        """
        CREATE INDEX IF NOT EXISTS blackboard_scope_gist
            ON blackboard USING GIST (scope_path);
        """,
        "CREATE INDEX IF NOT EXISTS blackboard_tenant_idx ON blackboard (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS blackboard_ttl_idx
            ON blackboard (ttl_until) WHERE ttl_until IS NOT NULL;
        """,
    ]


def _experiences_schema(vector_dim: int) -> list[str]:
    """Synapses table — Flat Memory Phase B.

    Stores agent action outcomes with Q-values for experience-driven
    agent improvement. Each experience carries a composite Q-score
    computed from action success, hypothesis quality, and context relevance.

    Partitioned by created_at (monthly) for efficient retention management.
    Use pg_partman for automatic partition creation, or manual:
        sudo apt install postgresql-16-partman
        CREATE EXTENSION pg_partman;
    Alternatively, create partitions via Worker schedule or init script.
    """
    return [
        """
        CREATE TABLE IF NOT EXISTS synapses (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       TEXT NOT NULL,
            agent_id        TEXT NOT NULL,
            graph_name      TEXT NULL,
            graph_run_id    UUID NULL,

            -- What was done
            action_type     TEXT NOT NULL,
            action_data     JSONB NOT NULL,
            context_summary TEXT NULL,
            client_id       TEXT NULL,

            -- Node classification (for credit assignment)
            node_role       TEXT NOT NULL DEFAULT 'worker'
                            CHECK (node_role IN ('planner','worker','terminal','router')),
            fault_class     TEXT NULL
                            CHECK (fault_class IN ('agent_fault','infra_fault','upstream_fault',
                                                   'policy_fault','reference_fault')),

            -- Lifecycle (OpenExp 8-state pattern)
            status          TEXT NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active','confirmed','outdated',
                                              'archived','contradicted','superseded','merged','deleted')),

            -- Outcome assessment (3-layer from OpenExp)
            q_action        DOUBLE PRECISION NOT NULL DEFAULT 0.5,
            q_hypothesis    DOUBLE PRECISION NOT NULL DEFAULT 0.5,
            q_relevance     DOUBLE PRECISION NOT NULL DEFAULT 0.5,
            q_composite     DOUBLE PRECISION GENERATED ALWAYS AS (
                                (q_action * 0.5) + (q_hypothesis * 0.3) + (q_relevance * 0.2)
                            ) STORED,

            -- Spatial + temporal
            scope_path      LTREE NULL,
            embedding       VECTOR(%d) NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
        % int(vector_dim),
        """
        CREATE INDEX IF NOT EXISTS synapses_q_composite_idx
            ON synapses (q_composite DESC);
        """,
        # Ranked-lookup index: query_synapses always scopes by tenant and
        # orders by q_composite DESC. Leading with tenant_id lets Postgres
        # seek straight to the tenant's slice and walk it pre-sorted, instead
        # of scanning the global q_composite index and filtering out every
        # other tenant's rows (read amplification that grows with total rows).
        """
        CREATE INDEX IF NOT EXISTS synapses_tenant_q_composite_idx
            ON synapses (tenant_id, q_composite DESC);
        """,
        """
        CREATE INDEX IF NOT EXISTS synapses_scope_gist
            ON synapses USING GIST (scope_path);
        """,
        "CREATE INDEX IF NOT EXISTS synapses_agent_idx ON synapses (agent_id);",
        "CREATE INDEX IF NOT EXISTS synapses_run_idx ON synapses (graph_run_id);",
        """
        CREATE INDEX IF NOT EXISTS synapses_status_idx
            ON synapses (status) WHERE status IN ('active', 'confirmed');
        """,
        "CREATE INDEX IF NOT EXISTS synapses_tenant_idx ON synapses (tenant_id);",
    ]


def _rls_policies() -> list[str]:
    """Row-Level Security policies for tenant isolation.

    Defence-in-depth (Layer 2): even if application-level tenant checks
    are bypassed, PostgreSQL itself blocks cross-tenant data access.

    Architecture:
        - ``brain_app`` role: used by gRPC handlers, RLS enforced
        - ``brain_admin`` role: used by Brain Admin / ContextForge dashboard access, bypasses RLS
        - Every query sets ``SET LOCAL app.current_tenant = '<tenant_id>'``
          before executing — this is done in the Brain gRPC interceptor.

    All statements are idempotent (IF NOT EXISTS / OR REPLACE / DROP IF EXISTS).
    """
    # All tables that have tenant_id column
    tenant_tables = [
        "cells",
        "cell_edges",
        "cell_aliases",
        "episodic_events",
        "user_facts",
        "event_journal",
        "catalog_taxonomy",
        "blackboard",
        "synapses",
    ]

    stmts: list[str] = []

    # 1. Create app role (non-superuser, no BYPASSRLS)
    stmts.append("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_app') THEN
                CREATE ROLE brain_app NOLOGIN;
                RAISE NOTICE 'Created role brain_app';
            END IF;
        END
        $$;
    """)

    # 2. Create admin role (bypasses RLS for Brain Admin / ContextForge dashboard access)
    stmts.append("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_admin') THEN
                CREATE ROLE brain_admin NOLOGIN BYPASSRLS;
                RAISE NOTICE 'Created role brain_admin';
            END IF;
        END
        $$;
    """)

    # 2b. Let the service DSN user assume brain_app (SET LOCAL ROLE in
    # set_tenant_context) and let brain_app resolve objects in the schema.
    # current_user/current_schema are evaluated at ensure_schema time,
    # i.e. the provisioning DSN user and the active search_path schema.
    stmts.append("""
        DO $$
        BEGIN
            IF current_user <> 'brain_app' THEN
                EXECUTE format('GRANT brain_app TO %I', current_user);
            END IF;
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO brain_app', current_schema());
            EXECUTE format('GRANT USAGE ON SCHEMA public TO brain_app');
        END
        $$;
    """)

    # Tables that require user-level isolation (B2C) inside a tenant
    user_isolated_tables = {
        "cells": (
            "user_id IS NULL OR user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
        "episodic_events": (
            "user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
        "user_facts": (
            "user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
        "event_journal": (
            "user_id IS NULL OR user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
    }

    # 3. Enable RLS and create policies for each tenant table
    for table in tenant_tables:
        policy_name = f"{table}_tenant_isolation"

        # Enable RLS (idempotent — no-op if already enabled)
        stmts.append(f"ALTER TABLE IF EXISTS {table} ENABLE ROW LEVEL SECURITY;")

        # Force RLS even for table owner (important!)
        stmts.append(f"ALTER TABLE IF EXISTS {table} FORCE ROW LEVEL SECURITY;")

        # Drop old policy (idempotent) then create
        stmts.append(f"DROP POLICY IF EXISTS {policy_name} ON {table};")

        # Check if table requires user isolation
        user_condition = user_isolated_tables.get(table)

        if user_condition:
            # Policy: rows visible/writable only when BOTH tenant_id AND user_id match session variables
            # '*' bypasses the specific user check (for admins or backend tasks)
            using_clause = f"""
                (tenant_id = current_setting('app.current_tenant', true) OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR ({user_condition})
                )
            """
            check_clause = f"""
                (tenant_id = current_setting('app.current_tenant', true) OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR ({user_condition})
                )
            """
        else:
            # Policy: rows visible only when tenant_id matches session variable
            using_clause = """
                tenant_id = current_setting('app.current_tenant', true)
                OR current_setting('app.current_tenant', true) = '*'
            """
            check_clause = using_clause

        stmts.append(f"""
            CREATE POLICY {policy_name} ON {table}
                USING (
                    {using_clause}
                )
                WITH CHECK (
                    {check_clause}
                );
        """)

        # Grant table access to brain_app role
        stmts.append(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO brain_app;")

        # Grant full access to brain_admin (bypasses RLS via BYPASSRLS flag)
        stmts.append(f"GRANT ALL ON {table} TO brain_admin;")

    return stmts


def build_rls_sql() -> Sequence[str]:
    """Return RLS policy statements for tenant isolation.

    Called as a separate step in ensure_schema AFTER table creation.
    Requires the connection user to be a superuser or table owner.
    """
    return _rls_policies()


def build_extension_sql() -> Sequence[str]:
    """Return CREATE EXTENSION statements (require superuser)."""
    return _extension_statements()


def build_preflight_rename_sql() -> Sequence[str]:
    """Return the breaking preflight rename statements.

    Called as the first DDL step in ensure_schema, before ``build_schema_sql()``,
    so a legacy database is converted in place instead of growing empty
    canonically-named tables alongside legacy ones.
    """
    return _preflight_rename_sql()


def build_schema_sql(
    *,
    vector_dim: int,
    include_commerce: bool = False,
) -> Sequence[str]:
    """Build schema SQL statements.

    Args:
        vector_dim: Dimension of embedding vectors:
            - 768 for all-mpnet-base-v2 (local)
            - 1536 for OpenAI text-embedding-3-small
            - 3072 for OpenAI text-embedding-3-large
        include_commerce: Include commerce/taxonomy tables

    Returns:
        List of SQL statements to execute
    """
    if vector_dim <= 0:
        raise BrainValidationError("vector_dim must be positive")

    statements = _core_schema(vector_dim)
    statements.extend(_blackboard_schema(vector_dim))
    statements.extend(_experiences_schema(vector_dim))

    if include_commerce:
        statements.extend(_commerce_schema(vector_dim))

    return statements


def build_column_backfill_sql() -> Sequence[str]:
    """Return ALTER TABLE statements that add columns missing from existing tables.

    Called as a separate step in ensure_schema AFTER the main DDL.
    Idempotent — safe to run on every startup.
    """
    stmts: list[str] = []
    stmts.extend(_column_backfill())
    stmts.extend(_constraint_upgrades())
    return stmts


__all__ = [
    "build_extension_sql",
    "build_preflight_rename_sql",
    "build_schema_sql",
    "build_column_backfill_sql",
    "build_rls_sql",
]
