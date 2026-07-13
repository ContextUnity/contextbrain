"""CP-1 breaking preflight: rename legacy Brain tables to canonical names.

Revision ID: 0009_cp1_storage_reset
Revises: 0008_agent_traces
Create Date: 2026-07-04

Implements the breaking preflight rename/drop policy: legacy physical names
are renamed in place — no compatibility views, no dual public names.
This is idempotent (every statement is guarded so
it is a no-op on a fresh database or one that already ran this migration) and
was verified against a populated pre-CP-1 database on 2026-07-04: data survives
and RLS policies remain enforced throughout. Step 6 drops the pre-CP-1 RLS
policy names (policies stay attached to a table across ``RENAME TO`` — they are
bound by OID, not name — so without this step the old policy would linger
alongside the canonical one forever, breaking physical-name parity with a
freshly created schema even though both enforce identical isolation logic).
See `services/brain/storage/postgres/schema.py`, which is the live source of
truth this migration mirrors as a frozen historical record.

Renames:
    knowledge_nodes    -> cells            (+ taxonomy_path column -> scope_path)
    knowledge_edges    -> cell_edges
    knowledge_aliases  -> cell_aliases
    blackboard_records -> blackboard
    agent_experiences  -> synapses
    agent_traces       -> event_journal    (+ Event Journal v0 columns)

Rollback boundary: downgrade() restores legacy physical names and drops the
Event Journal v0 columns. Per the recorded policy this is only a valid target
before CP-1 acceptance — after acceptance, legacy names are not a supported
rollback target and failures must be fixed forward against canonical storage.
"""

from alembic import op

# revision identifiers
revision = "0009_cp1_storage_reset"
down_revision = "0008_agent_traces"
branch_labels = None
depends_on = None


def _rename_table(old: str, new: str) -> str:
    # Keep textually identical to schema.py ``_rename_table`` (preflight parity).
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
    # Keep textually identical to schema.py ``_rename_index`` (preflight parity).
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
    # Scoped to the table and current schema — a bare conname lookup could
    # match a same-named constraint on another table/schema, making the
    # guard pass while the ALTER fails. Keep textually identical to
    # `_rename_constraint()` in services/brain/storage/postgres/schema.py
    # (test_schema_ddl.py::TestMigrationPreflightParity enforces this).
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
    return f"DROP POLICY IF EXISTS {old_policy} ON {table};"


def upgrade() -> None:
    # 1. Tables
    op.execute(_rename_table("knowledge_nodes", "cells"))
    op.execute(_rename_table("knowledge_edges", "cell_edges"))
    op.execute(_rename_table("knowledge_aliases", "cell_aliases"))
    op.execute(_rename_table("blackboard_records", "blackboard"))
    op.execute(_rename_table("agent_experiences", "synapses"))
    op.execute(_rename_table("agent_traces", "event_journal"))

    # 2. Column
    op.execute(_rename_column("cells", "taxonomy_path", "scope_path"))

    # 3. Indexes (PostgreSQL supports IF EXISTS on ALTER INDEX RENAME)
    op.execute(_rename_index("knowledge_nodes_pkey", "cells_pkey"))
    op.execute(_rename_index("knowledge_nodes_taxonomy_path_gist", "cells_scope_path_gist"))
    op.execute(_rename_index("knowledge_nodes_embedding_hnsw", "cells_embedding_hnsw"))
    op.execute(_rename_index("knowledge_nodes_search_vector_gin", "cells_search_vector_gin"))
    op.execute(_rename_index("knowledge_nodes_keywords_vector_gin", "cells_keywords_vector_gin"))
    op.execute(_rename_index("knowledge_nodes_source_type_idx", "cells_source_type_idx"))
    op.execute(_rename_index("knowledge_nodes_source_id_idx", "cells_source_id_idx"))
    op.execute(_rename_index("knowledge_nodes_node_kind_idx", "cells_node_kind_idx"))
    op.execute(_rename_index("knowledge_nodes_tenant_idx", "cells_tenant_idx"))
    op.execute(_rename_index("knowledge_nodes_struct_data_gin", "cells_struct_data_gin"))
    op.execute(
        _rename_index("knowledge_nodes_chunk_content_hash_uq", "cells_chunk_content_hash_uq")
    )

    op.execute(_rename_index("knowledge_edges_pkey", "cell_edges_pkey"))
    op.execute(_rename_index("knowledge_edges_source_idx", "cell_edges_source_idx"))
    op.execute(_rename_index("knowledge_edges_target_idx", "cell_edges_target_idx"))
    op.execute(_rename_index("knowledge_edges_relation_idx", "cell_edges_relation_idx"))
    op.execute(_rename_index("knowledge_edges_tenant_idx", "cell_edges_tenant_idx"))

    op.execute(_rename_index("knowledge_aliases_pkey", "cell_aliases_pkey"))
    op.execute(_rename_index("knowledge_aliases_node_id_idx", "cell_aliases_node_id_idx"))
    op.execute(_rename_index("knowledge_aliases_tenant_idx", "cell_aliases_tenant_idx"))

    op.execute(_rename_index("blackboard_records_pkey", "blackboard_pkey"))

    op.execute(_rename_index("agent_experiences_pkey", "synapses_pkey"))
    op.execute(_rename_index("experiences_q_composite_idx", "synapses_q_composite_idx"))
    op.execute(_rename_index("experiences_scope_gist", "synapses_scope_gist"))
    op.execute(_rename_index("experiences_agent_idx", "synapses_agent_idx"))
    op.execute(_rename_index("experiences_run_idx", "synapses_run_idx"))
    op.execute(_rename_index("experiences_status_idx", "synapses_status_idx"))
    op.execute(_rename_index("experiences_tenant_idx", "synapses_tenant_idx"))

    op.execute(_rename_index("agent_traces_pkey", "event_journal_pkey"))
    op.execute(_rename_index("agent_traces_tenant_idx", "event_journal_tenant_idx"))
    op.execute(_rename_index("agent_traces_agent_idx", "event_journal_agent_idx"))
    op.execute(_rename_index("agent_traces_session_idx", "event_journal_session_idx"))
    op.execute(_rename_index("agent_traces_created_idx", "event_journal_created_idx"))
    op.execute(_rename_index("agent_traces_tenant_created_idx", "event_journal_tenant_created_idx"))

    # 4. CHECK/FK constraints (exact legacy names verified against a live pre-CP-1 database)
    op.execute(
        _rename_constraint("cells", "knowledge_nodes_node_kind_check", "cells_node_kind_check")
    )
    op.execute(
        _rename_constraint("cells", "knowledge_nodes_source_type_check", "cells_source_type_check")
    )
    op.execute(
        _rename_constraint(
            "cell_edges", "knowledge_edges_source_id_fkey", "cell_edges_source_id_fkey"
        )
    )
    op.execute(
        _rename_constraint(
            "cell_edges", "knowledge_edges_target_id_fkey", "cell_edges_target_id_fkey"
        )
    )
    op.execute(
        _rename_constraint(
            "cell_aliases", "knowledge_aliases_node_id_fkey", "cell_aliases_node_id_fkey"
        )
    )
    op.execute(
        _rename_constraint(
            "synapses", "agent_experiences_node_role_check", "synapses_node_role_check"
        )
    )
    op.execute(
        _rename_constraint(
            "synapses", "agent_experiences_fault_class_check", "synapses_fault_class_check"
        )
    )
    op.execute(
        _rename_constraint("synapses", "agent_experiences_status_check", "synapses_status_check")
    )

    # 5. Event Journal v0 — trace-compatible append-only event storage
    # (storage columns only; no public Event Journal proto/SDK API yet).
    # IF EXISTS on the table keeps the "every statement is guarded" invariant:
    # within the alembic chain 0008 guarantees the table, but this revision must
    # also be a no-op when run standalone against a database without it.
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS event_id UUID DEFAULT gen_random_uuid();"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'trace.logged';"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'info';"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'recorded';"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD COLUMN IF NOT EXISTS source_refs JSONB NOT NULL DEFAULT '[]'::jsonb;"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal DROP CONSTRAINT IF EXISTS event_journal_event_id_key;"
    )
    op.execute(
        "ALTER TABLE IF EXISTS event_journal ADD CONSTRAINT event_journal_event_id_key UNIQUE (event_id);"
    )

    # 6. RLS policies created under the pre-CP-1 table names (verified against
    # a live legacy-upgraded database, 2026-07-04).
    op.execute(_drop_legacy_policy("cells", "knowledge_nodes_tenant_isolation"))
    op.execute(_drop_legacy_policy("cell_edges", "knowledge_edges_tenant_isolation"))
    op.execute(_drop_legacy_policy("cell_aliases", "knowledge_aliases_tenant_isolation"))
    op.execute(_drop_legacy_policy("blackboard", "blackboard_records_tenant_isolation"))
    op.execute(_drop_legacy_policy("synapses", "agent_experiences_tenant_isolation"))
    op.execute(_drop_legacy_policy("event_journal", "agent_traces_tenant_isolation"))


def downgrade() -> None:
    # Valid only before CP-1 acceptance (see module docstring).
    op.execute(
        "ALTER TABLE IF EXISTS event_journal DROP CONSTRAINT IF EXISTS event_journal_event_id_key;"
    )
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS source_refs;")
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS payload;")
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS status;")
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS severity;")
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS event_type;")
    op.execute("ALTER TABLE IF EXISTS event_journal DROP COLUMN IF EXISTS event_id;")

    op.execute(
        _rename_constraint("synapses", "synapses_status_check", "agent_experiences_status_check")
    )
    op.execute(
        _rename_constraint(
            "synapses", "synapses_fault_class_check", "agent_experiences_fault_class_check"
        )
    )
    op.execute(
        _rename_constraint(
            "synapses", "synapses_node_role_check", "agent_experiences_node_role_check"
        )
    )
    op.execute(
        _rename_constraint(
            "cell_aliases", "cell_aliases_node_id_fkey", "knowledge_aliases_node_id_fkey"
        )
    )
    op.execute(
        _rename_constraint(
            "cell_edges", "cell_edges_target_id_fkey", "knowledge_edges_target_id_fkey"
        )
    )
    op.execute(
        _rename_constraint(
            "cell_edges", "cell_edges_source_id_fkey", "knowledge_edges_source_id_fkey"
        )
    )
    op.execute(
        _rename_constraint("cells", "cells_source_type_check", "knowledge_nodes_source_type_check")
    )
    op.execute(
        _rename_constraint("cells", "cells_node_kind_check", "knowledge_nodes_node_kind_check")
    )

    op.execute(_rename_index("event_journal_tenant_created_idx", "agent_traces_tenant_created_idx"))
    op.execute(_rename_index("event_journal_created_idx", "agent_traces_created_idx"))
    op.execute(_rename_index("event_journal_session_idx", "agent_traces_session_idx"))
    op.execute(_rename_index("event_journal_agent_idx", "agent_traces_agent_idx"))
    op.execute(_rename_index("event_journal_tenant_idx", "agent_traces_tenant_idx"))
    op.execute(_rename_index("event_journal_pkey", "agent_traces_pkey"))

    op.execute(_rename_index("synapses_tenant_idx", "experiences_tenant_idx"))
    op.execute(_rename_index("synapses_status_idx", "experiences_status_idx"))
    op.execute(_rename_index("synapses_run_idx", "experiences_run_idx"))
    op.execute(_rename_index("synapses_agent_idx", "experiences_agent_idx"))
    op.execute(_rename_index("synapses_scope_gist", "experiences_scope_gist"))
    op.execute(_rename_index("synapses_q_composite_idx", "experiences_q_composite_idx"))
    op.execute(_rename_index("synapses_pkey", "agent_experiences_pkey"))

    op.execute(_rename_index("blackboard_pkey", "blackboard_records_pkey"))

    op.execute(_rename_index("cell_aliases_tenant_idx", "knowledge_aliases_tenant_idx"))
    op.execute(_rename_index("cell_aliases_node_id_idx", "knowledge_aliases_node_id_idx"))
    op.execute(_rename_index("cell_aliases_pkey", "knowledge_aliases_pkey"))

    op.execute(_rename_index("cell_edges_tenant_idx", "knowledge_edges_tenant_idx"))
    op.execute(_rename_index("cell_edges_relation_idx", "knowledge_edges_relation_idx"))
    op.execute(_rename_index("cell_edges_target_idx", "knowledge_edges_target_idx"))
    op.execute(_rename_index("cell_edges_source_idx", "knowledge_edges_source_idx"))
    op.execute(_rename_index("cell_edges_pkey", "knowledge_edges_pkey"))

    op.execute(
        _rename_index("cells_chunk_content_hash_uq", "knowledge_nodes_chunk_content_hash_uq")
    )
    op.execute(_rename_index("cells_struct_data_gin", "knowledge_nodes_struct_data_gin"))
    op.execute(_rename_index("cells_tenant_idx", "knowledge_nodes_tenant_idx"))
    op.execute(_rename_index("cells_node_kind_idx", "knowledge_nodes_node_kind_idx"))
    op.execute(_rename_index("cells_source_id_idx", "knowledge_nodes_source_id_idx"))
    op.execute(_rename_index("cells_source_type_idx", "knowledge_nodes_source_type_idx"))
    op.execute(_rename_index("cells_keywords_vector_gin", "knowledge_nodes_keywords_vector_gin"))
    op.execute(_rename_index("cells_search_vector_gin", "knowledge_nodes_search_vector_gin"))
    op.execute(_rename_index("cells_embedding_hnsw", "knowledge_nodes_embedding_hnsw"))
    op.execute(_rename_index("cells_scope_path_gist", "knowledge_nodes_taxonomy_path_gist"))
    op.execute(_rename_index("cells_pkey", "knowledge_nodes_pkey"))

    op.execute(_rename_column("cells", "scope_path", "taxonomy_path"))

    op.execute(_rename_table("event_journal", "agent_traces"))
    op.execute(_rename_table("synapses", "agent_experiences"))
    op.execute(_rename_table("blackboard", "blackboard_records"))
    op.execute(_rename_table("cell_aliases", "knowledge_aliases"))
    op.execute(_rename_table("cell_edges", "knowledge_edges"))
    op.execute(_rename_table("cells", "knowledge_nodes"))
