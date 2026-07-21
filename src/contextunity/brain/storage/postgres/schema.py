"""Postgres schema DDL for knowledge store (pgvector + ltree).

Modules:
- core: BrainCells, CellEdges, aliases, Conversation History (always included)

Canonical naming: tables and columns use the canonical Flat Memory nouns
(``cells``, ``cell_edges``, ``cell_aliases``, ``scope_path``, ``synapses``,
``execution_traces``, ``blackboard``). Legacy physical names (``knowledge_nodes``,
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
        "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
    ]


def _rename_table(old: str, new: str) -> str:
    """Idempotent table rename: only when old exists and new does not.

    ``ALTER TABLE IF EXISTS old RENAME TO new`` still fails if both tables
    already exist (partial migration). Guard both names in the current schema.
    """
    return f"""
        DO $$
        BEGIN
            IF to_regclass(format('%I.%I', current_schema(), '{old}')) IS NOT NULL
               AND to_regclass(format('%I.%I', current_schema(), '{new}')) IS NULL THEN
                ALTER TABLE {old} RENAME TO {new};
            END IF;
        END
        $$;
    """


def _rename_index(old: str, new: str) -> str:
    """Idempotent index rename: only when old exists and new does not."""
    return f"""
        DO $$
        BEGIN
            IF to_regclass(format('%I.%I', current_schema(), '{old}')) IS NOT NULL
               AND to_regclass(format('%I.%I', current_schema(), '{new}')) IS NULL THEN
                ALTER INDEX {old} RENAME TO {new};
            END IF;
        END
        $$;
    """


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


def _legacy_conversation_preflight_sql() -> str:
    """Return the atomic PostgreSQL legacy Conversation History preflight.

    PostgreSQL has no ``UPDATE ... IF TABLE EXISTS`` form. Keep the complete
    legacy-table decision and transformation in one ``DO`` block so a fresh
    schema is a no-op, a legacy schema is migrated as one statement, and a
    split-brain schema fails before canonical DDL can hide it.
    """
    return r"""
        DO $$
        DECLARE
            legacy_table regclass;
            canonical_table regclass;
        BEGIN
            legacy_table := to_regclass(
                format('%I.%I', current_schema(), 'episodic_events')
            );
            canonical_table := to_regclass(
                format('%I.%I', current_schema(), 'conversation_records')
            );

            IF legacy_table IS NOT NULL AND canonical_table IS NOT NULL THEN
                RAISE EXCEPTION 'both legacy and canonical conversation tables exist';
            END IF;

            IF legacy_table IS NOT NULL THEN
                IF EXISTS (
                    SELECT 1
                    FROM episodic_events
                    WHERE id::text !~
                            '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
                       OR tenant_id IS NULL OR tenant_id = ''
                       OR user_id IS NULL OR user_id = ''
                       OR content IS NULL
                       OR jsonb_typeof(COALESCE(metadata, '{}'::jsonb)) <> 'object'
                ) THEN
                    RAISE EXCEPTION 'malformed legacy conversation row blocks migration'
                        USING ERRCODE = '23514';
                END IF;

                ALTER TABLE episodic_events RENAME TO conversation_records;
                canonical_table := to_regclass(
                    format('%I.%I', current_schema(), 'conversation_records')
                );
            END IF;

            IF canonical_table IS NULL THEN
                RETURN;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'conversation_records'
                  AND column_name = 'id'
            ) THEN
                ALTER TABLE conversation_records RENAME COLUMN id TO record_id;
            END IF;

            ALTER TABLE conversation_records
                ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'legacy',
                ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'legacy_import',
                ADD COLUMN IF NOT EXISTS content_hash TEXT,
                ADD COLUMN IF NOT EXISTS source_hash TEXT,
                ADD COLUMN IF NOT EXISTS graph_run_id UUID,
                ADD COLUMN IF NOT EXISTS metadata_version INTEGER NOT NULL DEFAULT 1,
                ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

            ALTER TABLE conversation_records
                ALTER COLUMN record_id TYPE UUID USING record_id::text::uuid;

            UPDATE conversation_records
            SET role = COALESCE(
                    NULLIF(metadata #>> '{_conversation_migration,role}', ''),
                    role
                ),
                kind = COALESCE(
                    NULLIF(metadata #>> '{_conversation_migration,kind}', ''),
                    kind
                ),
                content_hash = COALESCE(
                    content_hash,
                    'sha256:' || encode(sha256(convert_to(content, 'UTF8')), 'hex')
                );

            UPDATE conversation_records
            SET source_hash = CASE
                    WHEN source_hash IS NOT NULL THEN source_hash
                    WHEN metadata #>> '{_conversation_migration,source_hash}'
                        ~ '^sha256:[0-9a-f]{64}$'
                        THEN metadata #>> '{_conversation_migration,source_hash}'
                    WHEN metadata->>'source_hash' ~ '^sha256:[0-9a-f]{64}$'
                        THEN metadata->>'source_hash'
                    ELSE content_hash
                END,
                graph_run_id = CASE
                    WHEN graph_run_id IS NOT NULL THEN graph_run_id
                    WHEN metadata #>> '{_conversation_migration,graph_run_id}'
                        ~ '^[0-9a-fA-F-]{36}$'
                        THEN (
                            metadata #>> '{_conversation_migration,graph_run_id}'
                        )::uuid
                    WHEN metadata->>'graph_run_id' ~ '^[0-9a-fA-F-]{36}$'
                        THEN (metadata->>'graph_run_id')::uuid
                    ELSE NULL
                END,
                idempotency_key = COALESCE(
                    idempotency_key,
                    NULLIF(
                        metadata #>> '{_conversation_migration,idempotency_key}',
                        ''
                    ),
                    'legacy:' || record_id::text
                ),
                metadata = metadata - '_conversation_migration';

            ALTER TABLE conversation_records
                ALTER COLUMN content_hash SET NOT NULL,
                ALTER COLUMN source_hash SET NOT NULL,
                ALTER COLUMN idempotency_key SET NOT NULL,
                DROP COLUMN IF EXISTS embedding;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'conversation_records_role_check'
                  AND conrelid = canonical_table
            ) THEN
                ALTER TABLE conversation_records
                    ADD CONSTRAINT conversation_records_role_check
                    CHECK (role IN ('user','assistant','system','tool','legacy'));
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'conversation_records_kind_check'
                  AND conrelid = canonical_table
            ) THEN
                ALTER TABLE conversation_records
                    ADD CONSTRAINT conversation_records_kind_check
                    CHECK (kind IN (
                        'message','turn_summary','conversation_note','legacy_import'
                    ));
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'conversation_records_content_hash_check'
                  AND conrelid = canonical_table
            ) THEN
                ALTER TABLE conversation_records
                    ADD CONSTRAINT conversation_records_content_hash_check
                    CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$');
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'conversation_records_source_hash_check'
                  AND conrelid = canonical_table
            ) THEN
                ALTER TABLE conversation_records
                    ADD CONSTRAINT conversation_records_source_hash_check
                    CHECK (source_hash ~ '^sha256:[0-9a-f]{64}$');
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'conversation_records_metadata_version_check'
                  AND conrelid = canonical_table
            ) THEN
                ALTER TABLE conversation_records
                    ADD CONSTRAINT conversation_records_metadata_version_check
                    CHECK (metadata_version = 1);
            END IF;

            DROP INDEX IF EXISTS episodic_events_embedding_hnsw;
            DROP INDEX IF EXISTS episodic_events_user_idx;
            DROP INDEX IF EXISTS episodic_events_session_idx;
            DROP INDEX IF EXISTS episodic_events_tenant_idx;
            CREATE UNIQUE INDEX IF NOT EXISTS
                conversation_records_tenant_idempotency_idx
                ON conversation_records (tenant_id, idempotency_key);
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
    stmts.append(_legacy_conversation_preflight_sql())
    stmts.append(
        """
        DO $$
        DECLARE
            has_unknown boolean := false;
        BEGIN
            IF to_regclass(format('%I.%I', current_schema(), 'event_journal')) IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema()
                     AND table_name = 'event_journal'
                     AND column_name = 'event_type'
               ) THEN
                EXECUTE 'SELECT EXISTS (SELECT 1 FROM event_journal
                    WHERE event_type <> ''trace.logged''
                       OR severity <> ''info''
                       OR status <> ''recorded''
                       OR payload <> ''{}''::jsonb
                       OR source_refs <> ''[]''::jsonb)'
                    INTO has_unknown;
                IF has_unknown THEN
                    RAISE EXCEPTION 'unmapped generic event row blocks trace migration';
                END IF;
            END IF;
        END
        $$;
        """
    )
    stmts.append(_rename_table("event_journal", "execution_traces"))

    # 2. Columns (table must already be renamed by step 1).
    stmts.append(_rename_column("cells", "taxonomy_path", "scope_path"))
    stmts.append(_rename_column("cells", "node_kind", "cell_kind"))

    # 3. PK-backing and secondary indexes.
    stmts.append(_rename_index("knowledge_nodes_pkey", "cells_pkey"))
    stmts.append(_rename_index("knowledge_nodes_taxonomy_path_gist", "cells_scope_path_gist"))
    stmts.append(_rename_index("knowledge_nodes_embedding_hnsw", "cells_embedding_hnsw"))
    stmts.append(_rename_index("knowledge_nodes_search_vector_gin", "cells_search_vector_gin"))
    stmts.append(_rename_index("knowledge_nodes_keywords_vector_gin", "cells_keywords_vector_gin"))
    stmts.append(_rename_index("knowledge_nodes_source_type_idx", "cells_source_type_idx"))
    stmts.append(_rename_index("knowledge_nodes_source_id_idx", "cells_source_id_idx"))
    stmts.append(_rename_index("knowledge_nodes_node_kind_idx", "cells_cell_kind_idx"))
    stmts.append(_rename_index("cells_node_kind_idx", "cells_cell_kind_idx"))
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

    stmts.append(_rename_index("agent_traces_pkey", "execution_traces_pkey"))
    stmts.append(_rename_index("agent_traces_tenant_idx", "execution_traces_tenant_idx"))
    stmts.append(_rename_index("agent_traces_agent_idx", "execution_traces_agent_idx"))
    stmts.append(_rename_index("agent_traces_session_idx", "execution_traces_session_idx"))
    stmts.append(_rename_index("agent_traces_created_idx", "execution_traces_created_idx"))
    stmts.append(
        _rename_index("agent_traces_tenant_created_idx", "execution_traces_tenant_created_idx")
    )
    stmts.append(_rename_index("event_journal_pkey", "execution_traces_pkey"))
    stmts.append(_rename_index("event_journal_tenant_idx", "execution_traces_tenant_idx"))
    stmts.append(_rename_index("event_journal_agent_idx", "execution_traces_agent_idx"))
    stmts.append(_rename_index("event_journal_session_idx", "execution_traces_session_idx"))
    stmts.append(_rename_index("event_journal_created_idx", "execution_traces_created_idx"))
    stmts.append(
        _rename_index("event_journal_tenant_created_idx", "execution_traces_tenant_created_idx")
    )

    # 4. CHECK/FK constraints (verified against a live legacy-upgraded database, 2026-07-04).
    stmts.append(
        _rename_constraint("cells", "knowledge_nodes_node_kind_check", "cells_cell_kind_check")
    )
    stmts.append(_rename_constraint("cells", "cells_node_kind_check", "cells_cell_kind_check"))
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
    stmts.append(_drop_legacy_policy("execution_traces", "agent_traces_tenant_isolation"))
    stmts.append(_drop_legacy_policy("execution_traces", "event_journal_tenant_isolation"))

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
            cell_kind       TEXT NOT NULL CHECK (cell_kind IN (
                'chunk','concept','fact','preference','config','document',
                'documentation','entity','summary'
            )),

            source_type     TEXT NULL CHECK (source_type IN (
                'video','book','qa','web','knowledge','documentation','product','dealer_product',
                'auto_extract','manual','synthesis','test','config','runbook',
                'memory','tool','retention'
            )),
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
        "CREATE INDEX IF NOT EXISTS cells_cell_kind_idx ON cells (cell_kind);",
        "CREATE INDEX IF NOT EXISTS cells_tenant_idx ON cells (tenant_id);",
        """
        CREATE INDEX IF NOT EXISTS cells_struct_data_gin
          ON cells USING GIN (struct_data);
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS cells_chunk_content_hash_uq
          ON cells (cell_kind, content_hash)
          WHERE cell_kind = 'chunk' AND content_hash IS NOT NULL;
        """,
        # Durable asynchronous embedding ledger. It stores references only.
        """
        CREATE TABLE IF NOT EXISTS cell_embedding_jobs (
            job_id          TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            cell_id         TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
            content_hash    TEXT NOT NULL,
            profile         TEXT NOT NULL,
            status          TEXT NOT NULL CHECK (status IN ('pending','processing','ready','failed','skipped')),
            attempt         INTEGER NOT NULL DEFAULT 0,
            lease_id        TEXT,
            lease_until     TIMESTAMPTZ,
            error_code      TEXT,
            idempotency_key TEXT NOT NULL UNIQUE,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS embedding_jobs_claim_idx ON cell_embedding_jobs (tenant_id, status, lease_until);",
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
        # Conversation History
        """
        CREATE TABLE IF NOT EXISTS conversation_records (
            record_id        UUID PRIMARY KEY,
            tenant_id        TEXT NOT NULL,
            user_id          TEXT NOT NULL,
            session_id       TEXT NULL,
            role             TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool','legacy')),
            kind             TEXT NOT NULL CHECK (kind IN ('message','turn_summary','conversation_note','legacy_import')),
            content          TEXT NOT NULL,
            content_hash     TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
            source_hash      TEXT NOT NULL CHECK (source_hash ~ '^sha256:[0-9a-f]{64}$'),
            graph_run_id     UUID NULL,
            metadata_version INTEGER NOT NULL CHECK (metadata_version = 1),
            idempotency_key  TEXT NOT NULL,
            metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, idempotency_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS conversation_records_tenant_user_time_idx ON conversation_records (tenant_id, user_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS conversation_records_tenant_session_time_idx ON conversation_records (tenant_id, session_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS conversation_records_tenant_run_idx ON conversation_records (tenant_id, graph_run_id);",
        """
        CREATE TABLE IF NOT EXISTS conversation_migration_receipts (
            migration_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            source_count BIGINT NOT NULL,
            target_count BIGINT NOT NULL,
            source_digest TEXT NOT NULL,
            target_digest TEXT NOT NULL,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (migration_id, tenant_id),
            CHECK (source_count = target_count),
            CHECK (source_digest = target_digest)
        );
        """,
        # Execution Traces — one terminal record per graph run.
        """
        CREATE TABLE IF NOT EXISTS execution_traces (
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
            graph_run_id    UUID NULL,
            payload_digest  TEXT NULL,
            terminal_status TEXT NULL CHECK (terminal_status IN ('succeeded','failed','cancelled')),
            terminal_reason TEXT NULL,
            trace_schema_version TEXT NOT NULL DEFAULT 'legacy_v0',
            prompt_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
            steps           JSONB NOT NULL DEFAULT '[]'::jsonb,
            control_evidence JSONB NOT NULL DEFAULT '{}'::jsonb
                CONSTRAINT execution_traces_control_evidence_object_check
                CHECK (jsonb_typeof(control_evidence) = 'object'),
            final_verdict JSONB NOT NULL DEFAULT '{}'::jsonb
                CONSTRAINT execution_traces_final_verdict_object_check
                CHECK (jsonb_typeof(final_verdict) = 'object'),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS execution_traces_tenant_idx ON execution_traces (tenant_id);",
        "CREATE INDEX IF NOT EXISTS execution_traces_agent_idx ON execution_traces (agent_id);",
        "CREATE INDEX IF NOT EXISTS execution_traces_session_idx ON execution_traces (session_id);",
        "CREATE INDEX IF NOT EXISTS execution_traces_created_idx ON execution_traces (created_at DESC);",
        "CREATE INDEX IF NOT EXISTS execution_traces_tenant_created_idx ON execution_traces (tenant_id, created_at DESC);",
        "CREATE UNIQUE INDEX IF NOT EXISTS execution_traces_tenant_id_uq ON execution_traces (tenant_id, id);",
        """
        CREATE TABLE IF NOT EXISTS outcome_observations (
            observation_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            trace_id UUID NOT NULL,
            graph_run_id UUID NOT NULL,
            verdict_digest TEXT NOT NULL CHECK (verdict_digest ~ '^[0-9a-f]{64}$'),
            observation_kind TEXT NOT NULL CHECK (observation_kind IN ('verified_success','verified_failure','neutral')),
            source_authority TEXT NOT NULL CHECK (source_authority = 'operator_review/v1'),
            source_ref TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            idempotency_key TEXT NOT NULL,
            canonical_digest TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            policy_version TEXT NOT NULL,
            resolution_receipt JSONB NOT NULL CHECK (jsonb_typeof(resolution_receipt) = 'object'),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT outcome_observations_trace_scope_fk
                FOREIGN KEY (tenant_id, trace_id)
                REFERENCES execution_traces(tenant_id, id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, source_authority, idempotency_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS outcome_observations_trace_idx ON outcome_observations (tenant_id, trace_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS outcome_observations_tenant_id_uq ON outcome_observations (tenant_id, observation_id);",
        """
        CREATE TABLE IF NOT EXISTS execution_trace_artifacts (
            artifact_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            trace_id UUID NOT NULL,
            graph_run_id UUID NOT NULL,
            invocation_id UUID NOT NULL,
            provider_attempt_id UUID NOT NULL,
            artifact_kind TEXT NOT NULL CHECK (artifact_kind = 'model_io'),
            content_schema TEXT NOT NULL CHECK (content_schema = 'contextunity.model-io-content/v1'),
            capture_state TEXT NOT NULL CHECK (capture_state IN ('captured','disabled','redacted','rejected','unavailable')),
            storage_state TEXT NOT NULL CHECK (storage_state IN ('hot','archiving','cold','restoring','purging','purged')),
            lifecycle_profile_id TEXT NOT NULL,
            content_digest TEXT NOT NULL,
            reservation_digest TEXT NOT NULL,
            protected_envelope JSONB NULL,
            archive_receipt JSONB NULL,
            request_bytes BIGINT NOT NULL CHECK (request_bytes >= 0),
            response_bytes BIGINT NOT NULL CHECK (response_bytes >= 0),
            revision BIGINT NOT NULL CHECK (revision >= 1),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            purged_at TIMESTAMPTZ NULL,
            UNIQUE (tenant_id, project_id, graph_run_id, provider_attempt_id, artifact_kind)
        );
        """,
        "CREATE INDEX IF NOT EXISTS execution_trace_artifacts_trace_idx ON execution_trace_artifacts (tenant_id, project_id, trace_id);",
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
        "ALTER TABLE execution_traces ALTER COLUMN id SET DEFAULT gen_random_uuid();",
        # Execution Trace terminal-snapshot columns. Existing rows remain legacy_v0.
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS provenance TEXT[] NULL;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS graph_run_id UUID NULL;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS payload_digest TEXT NULL;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS terminal_status TEXT NULL;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS terminal_reason TEXT NULL;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS trace_schema_version TEXT NOT NULL DEFAULT 'legacy_v0';",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS prompt_evidence JSONB NOT NULL DEFAULT '[]'::jsonb;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS steps JSONB NOT NULL DEFAULT '[]'::jsonb;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS control_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS final_verdict JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_traces_final_verdict_object_check' AND conrelid = 'execution_traces'::regclass) THEN ALTER TABLE execution_traces ADD CONSTRAINT execution_traces_final_verdict_object_check CHECK (jsonb_typeof(final_verdict) = 'object'); END IF; END $$;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'execution_traces_control_evidence_object_check' AND conrelid = 'execution_traces'::regclass) THEN ALTER TABLE execution_traces ADD CONSTRAINT execution_traces_control_evidence_object_check CHECK (jsonb_typeof(control_evidence) = 'object'); END IF; END $$;",
        "CREATE UNIQUE INDEX IF NOT EXISTS execution_traces_tenant_id_uq ON execution_traces (tenant_id, id);",
        "ALTER TABLE outcome_observations DROP CONSTRAINT IF EXISTS outcome_observations_trace_id_fkey;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'outcome_observations_trace_scope_fk' AND conrelid = 'outcome_observations'::regclass) THEN ALTER TABLE outcome_observations ADD CONSTRAINT outcome_observations_trace_scope_fk FOREIGN KEY (tenant_id, trace_id) REFERENCES execution_traces(tenant_id, id) ON DELETE RESTRICT; END IF; END $$;",
        "CREATE UNIQUE INDEX IF NOT EXISTS synapses_tenant_id_uq ON synapses (tenant_id, id);",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'outcome_synapse_effects_synapse_scope_fk' AND conrelid = 'outcome_synapse_effects'::regclass) THEN ALTER TABLE outcome_synapse_effects ADD CONSTRAINT outcome_synapse_effects_synapse_scope_fk FOREIGN KEY (tenant_id, synapse_id) REFERENCES synapses(tenant_id, id) ON DELETE RESTRICT; END IF; END $$;",
        "CREATE UNIQUE INDEX IF NOT EXISTS execution_traces_tenant_run_uq ON execution_traces (tenant_id, graph_run_id) WHERE graph_run_id IS NOT NULL;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS event_id;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS event_type;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS severity;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS status;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS payload;",
        "ALTER TABLE execution_traces DROP COLUMN IF EXISTS source_refs;",
        # cells — Phase 3 BrainCell metadata columns
        "ALTER TABLE cells ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();",
        "ALTER TABLE cells ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5;",
        "ALTER TABLE cells ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'tenant';",
        "ALTER TABLE cells ADD COLUMN IF NOT EXISTS source_ref TEXT NULL;",
        # synapses — canonical BrainSynapse contract fields
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS action_data_ref TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS thought_trace_ref TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS content_hash TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS node_id TEXT NULL;",
        "ALTER TABLE synapses ADD COLUMN IF NOT EXISTS node_name TEXT NULL;",
        # UDB mutation receipts added after the original UDB schema. Existing
        # rows remain NULL and are rejected on retry rather than silently
        # accepting a command whose immutable payload cannot be proven.
        "ALTER TABLE debug_case_mitigations ADD COLUMN IF NOT EXISTS canonical_digest TEXT NULL;",
        "ALTER TABLE debug_case_transitions ADD COLUMN IF NOT EXISTS canonical_digest TEXT NULL;",
    ]


def _constraint_upgrades() -> list[str]:
    """Idempotent constraint updates for evolved schemas.

    CHECK constraints and other DDL that may need updating on existing tables.
    Each statement is wrapped to be safe on re-run.
    """
    return [
        # cells — Phase 3 source_type and cell_kind values
        "ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_source_type_check;",
        """ALTER TABLE cells ADD CONSTRAINT cells_source_type_check
           CHECK (source_type IS NULL OR source_type IN (
               'video','book','qa','web','knowledge','documentation','product','dealer_product',
               'auto_extract','manual','synthesis','test','config','runbook',
               'memory','tool','retention'
           ));""",
        "ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_cell_kind_check;",
        """ALTER TABLE cells ADD CONSTRAINT cells_cell_kind_check
           CHECK (cell_kind IN (
               'chunk','concept','fact','preference','config','document',
               'documentation','entity','summary'
           ));""",
        "ALTER TABLE execution_traces DROP CONSTRAINT IF EXISTS execution_traces_terminal_status_check;",
        "ALTER TABLE execution_traces ADD CONSTRAINT execution_traces_terminal_status_check CHECK (terminal_status IS NULL OR terminal_status IN ('succeeded','failed','cancelled'));",
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


def _synapses_schema(vector_dim: int) -> list[str]:
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
        "CREATE UNIQUE INDEX IF NOT EXISTS synapses_tenant_id_uq ON synapses (tenant_id, id);",
        """
        CREATE TABLE IF NOT EXISTS outcome_synapse_effects (
            effect_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            observation_id UUID NOT NULL,
            synapse_id UUID NOT NULL,
            source_authority TEXT NOT NULL CHECK (source_authority = 'operator_review/v1'),
            idempotency_key TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT outcome_synapse_effects_observation_scope_fk
                FOREIGN KEY (tenant_id, observation_id)
                REFERENCES outcome_observations(tenant_id, observation_id)
                ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT outcome_synapse_effects_synapse_scope_fk
                FOREIGN KEY (tenant_id, synapse_id)
                REFERENCES synapses(tenant_id, id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, source_authority, idempotency_key, synapse_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS outcome_synapse_effects_observation_idx ON outcome_synapse_effects (tenant_id, observation_id);",
    ]


def _udb_schema() -> list[str]:
    """Return UDB tables; they are a separate negative-experience authority."""
    return [
        """
        CREATE TABLE IF NOT EXISTS debug_cases (
            case_id             UUID PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL CHECK (fingerprint_version = 'contextunity.udb-fingerprint/v1'),
            fingerprint         TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
            fault_class         TEXT NOT NULL CHECK (fault_class IN ('agent_fault','infra_fault','upstream_fault','policy_fault','reference_fault')),
            operation_kind      TEXT NOT NULL CHECK (operation_kind IN ('brain_search','auto_extract','secure_node','synapse_record','memory_synthesis','embedding_enrichment')),
            policy_version      TEXT NOT NULL CHECK (policy_version = 'contextunity.error-evidence/v1'),
            comparison_key      JSONB NOT NULL,
            state               TEXT NOT NULL CHECK (state IN ('open','resolved')),
            fault_count         INTEGER NOT NULL CHECK (fault_count >= 1),
            success_count       INTEGER NOT NULL CHECK (success_count >= 0),
            q_error             DOUBLE PRECISION NOT NULL CHECK (q_error >= 0.0 AND q_error <= 1.0),
            case_revision       INTEGER NOT NULL CHECK (case_revision >= 1),
            first_occurred_at   TIMESTAMPTZ NOT NULL,
            last_occurred_at    TIMESTAMPTZ NOT NULL,
            resolved_at         TIMESTAMPTZ NULL,
            UNIQUE (tenant_id, case_id),
            UNIQUE (tenant_id, fingerprint_version, fingerprint)
        );
        """,
        "CREATE INDEX IF NOT EXISTS debug_cases_tenant_state_idx ON debug_cases (tenant_id, state, last_occurred_at DESC);",
        """
        CREATE TABLE IF NOT EXISTS debug_case_occurrences (
            occurrence_id       UUID PRIMARY KEY,
            case_id             UUID NOT NULL,
            tenant_id           TEXT NOT NULL,
            producer_id         TEXT NOT NULL,
            idempotency_key     TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL,
            fingerprint         TEXT NOT NULL,
            fault_class         TEXT NOT NULL,
            operation_kind      TEXT NOT NULL,
            fault_code          TEXT NOT NULL,
            policy_version      TEXT NOT NULL,
            comparison_key      JSONB NOT NULL,
            trace_id            UUID NULL,
            graph_run_id        UUID NULL,
            node_id             TEXT NULL,
            step_id             UUID NULL,
            occurred_at         TIMESTAMPTZ NOT NULL,
            canonical_digest    TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, case_id, occurrence_id),
            UNIQUE (tenant_id, producer_id, idempotency_key)
        );
        """,
        "CREATE INDEX IF NOT EXISTS debug_occurrences_tenant_case_idx ON debug_case_occurrences (tenant_id, case_id, occurred_at DESC);",
        """
        CREATE TABLE IF NOT EXISTS debug_case_recoveries (
            recovery_id            UUID PRIMARY KEY,
            case_id                UUID NOT NULL,
            tenant_id              TEXT NOT NULL,
            policy_version         TEXT NOT NULL,
            comparison_key         JSONB NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            exposure_id            TEXT NOT NULL,
            kind                   TEXT NOT NULL CHECK (kind IN ('verified_recovery_probe','comparable_success')),
            verified_at            TIMESTAMPTZ NOT NULL,
            canonical_digest       TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, exposure_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS debug_recoveries_tenant_case_idx ON debug_case_recoveries (tenant_id, case_id);",
        """
        CREATE TABLE IF NOT EXISTS debug_case_transitions (
            transition_id          TEXT PRIMARY KEY,
            case_id                UUID NOT NULL,
            tenant_id              TEXT NOT NULL,
            transition_kind        TEXT NOT NULL CHECK (transition_kind IN ('resolved','reopened')),
            expected_case_revision INTEGER NOT NULL,
            trigger_occurrence_id  UUID NULL,
            transitioned_at        TIMESTAMPTZ NOT NULL,
            canonical_digest       TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            FOREIGN KEY (tenant_id, case_id, trigger_occurrence_id)
                REFERENCES debug_case_occurrences(tenant_id, case_id, occurrence_id) ON DELETE RESTRICT,
            UNIQUE (case_id, transition_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS debug_transitions_tenant_case_idx ON debug_case_transitions (tenant_id, case_id);",
        """
        CREATE TABLE IF NOT EXISTS debug_case_mitigations (
            attempt_id             UUID PRIMARY KEY,
            case_id                UUID NOT NULL,
            tenant_id              TEXT NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            kind                   TEXT NOT NULL CHECK (kind IN ('retry','mitigation','manual_probe')),
            idempotency_key        TEXT NOT NULL,
            attempted_at           TIMESTAMPTZ NOT NULL,
            canonical_digest        TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, idempotency_key)
        );
        """,
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
        "conversation_records",
        "conversation_migration_receipts",
        "execution_traces",
        "outcome_observations",
        "outcome_synapse_effects",
        "execution_trace_artifacts",
        "blackboard",
        "synapses",
        "cell_embedding_jobs",
        "debug_cases",
        "debug_case_occurrences",
        "debug_case_recoveries",
        "debug_case_transitions",
        "debug_case_mitigations",
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
        "conversation_records": (
            "user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
        "execution_traces": (
            "user_id IS NULL OR user_id = current_setting('app.current_user', true) "
            "OR current_setting('app.current_user', true) = '*'"
        ),
    }

    # The role switch in ``set_tenant_context`` must retain access to the
    # configured schema. Without USAGE PostgreSQL silently resolves unqualified
    # table names against public after SET LOCAL ROLE.
    stmts.append(
        """
        DO $$
        DECLARE schema_name TEXT := current_schema();
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_app') THEN
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO brain_app', schema_name);
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_admin') THEN
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO brain_admin', schema_name);
            END IF;
        END
        $$;
        """
    )

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
) -> Sequence[str]:
    """Build schema SQL statements.

    Args:
        vector_dim: Dimension of embedding vectors:
            - 768 for all-mpnet-base-v2 (local)
            - 1536 for OpenAI text-embedding-3-small
            - 3072 for OpenAI text-embedding-3-large

    Returns:
        List of SQL statements to execute
    """
    if vector_dim <= 0:
        raise BrainValidationError("vector_dim must be positive")

    statements = _core_schema(vector_dim)
    statements.extend(_blackboard_schema(vector_dim))
    statements.extend(_synapses_schema(vector_dim))
    statements.extend(_udb_schema())

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
