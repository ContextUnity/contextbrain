"""Canonicalize terminal execution trace storage.

Revision ID: 0015_execution_traces
Revises: 0014_cell_kind_column
Create Date: 2026-07-16
"""

from collections.abc import Sequence

from alembic import op

revision = "0015_execution_traces"
down_revision = "0014_cell_kind_column"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Reject unknown generic events and move recognized traces in place."""
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('execution_traces') IS NULL THEN
                IF EXISTS (
                    SELECT 1 FROM event_journal
                    WHERE event_type <> 'trace.logged'
                       OR severity <> 'info'
                       OR status <> 'recorded'
                       OR payload <> '{}'::jsonb
                       OR source_refs <> '[]'::jsonb
                ) THEN
                    RAISE EXCEPTION 'unmapped generic event row blocks trace migration';
                END IF;
                ALTER TABLE event_journal RENAME TO execution_traces;
            ELSIF to_regclass('event_journal') IS NOT NULL THEN
                IF EXISTS (SELECT 1 FROM event_journal) THEN
                    RAISE EXCEPTION 'event_journal and execution_traces both exist with legacy rows';
                END IF;
                DROP TABLE event_journal;
            END IF;
        END $$;
        ALTER INDEX IF EXISTS event_journal_pkey RENAME TO execution_traces_pkey;
        ALTER INDEX IF EXISTS event_journal_tenant_idx RENAME TO execution_traces_tenant_idx;
        ALTER INDEX IF EXISTS event_journal_agent_idx RENAME TO execution_traces_agent_idx;
        ALTER INDEX IF EXISTS event_journal_session_idx RENAME TO execution_traces_session_idx;
        ALTER INDEX IF EXISTS event_journal_created_idx RENAME TO execution_traces_created_idx;
        ALTER INDEX IF EXISTS event_journal_tenant_created_idx
            RENAME TO execution_traces_tenant_created_idx;
        ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS graph_run_id UUID NULL;
        ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS payload_digest TEXT NULL;
        ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS terminal_status TEXT NULL;
        ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS terminal_reason TEXT NULL;
        ALTER TABLE execution_traces
            ADD COLUMN IF NOT EXISTS trace_schema_version TEXT NOT NULL DEFAULT 'legacy_v0';
        ALTER TABLE execution_traces
            ADD COLUMN IF NOT EXISTS prompt_evidence JSONB NOT NULL DEFAULT '[]'::jsonb;
        ALTER TABLE execution_traces
            ADD COLUMN IF NOT EXISTS steps JSONB NOT NULL DEFAULT '[]'::jsonb;
        ALTER TABLE execution_traces ALTER COLUMN id SET DEFAULT gen_random_uuid();
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS event_id;
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS event_type;
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS severity;
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS status;
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS payload;
        ALTER TABLE execution_traces DROP COLUMN IF EXISTS source_refs;
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'execution_traces_terminal_status_check'
                  AND conrelid = 'execution_traces'::regclass
            ) THEN
                ALTER TABLE execution_traces
                    ADD CONSTRAINT execution_traces_terminal_status_check
                    CHECK (terminal_status IS NULL OR terminal_status IN ('succeeded','failed','cancelled'));
            END IF;
        END $$;
        CREATE UNIQUE INDEX IF NOT EXISTS execution_traces_tenant_run_uq
            ON execution_traces (tenant_id, graph_run_id)
            WHERE graph_run_id IS NOT NULL;
        ALTER TABLE execution_traces ENABLE ROW LEVEL SECURITY;
        ALTER TABLE execution_traces FORCE ROW LEVEL SECURITY;
        DROP POLICY IF EXISTS agent_traces_tenant_isolation ON execution_traces;
        DROP POLICY IF EXISTS event_journal_tenant_isolation ON execution_traces;
        DROP POLICY IF EXISTS execution_traces_tenant_isolation ON execution_traces;
        CREATE POLICY execution_traces_tenant_isolation ON execution_traces
            USING (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            )
            WITH CHECK (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            );
        """
    )


def downgrade() -> None:
    """Restore the legacy physical name without discarding terminal columns."""
    op.execute(
        """
        DROP POLICY IF EXISTS execution_traces_tenant_isolation ON execution_traces;
        DROP POLICY IF EXISTS event_journal_tenant_isolation ON execution_traces;
        DROP POLICY IF EXISTS agent_traces_tenant_isolation ON execution_traces;
        CREATE POLICY event_journal_tenant_isolation ON execution_traces
            USING (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            )
            WITH CHECK (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            );
        DROP INDEX IF EXISTS execution_traces_tenant_run_uq;
        ALTER TABLE execution_traces
            DROP CONSTRAINT IF EXISTS execution_traces_terminal_status_check;
        ALTER TABLE execution_traces ADD COLUMN event_id UUID DEFAULT gen_random_uuid();
        ALTER TABLE execution_traces
            ADD COLUMN event_type TEXT NOT NULL DEFAULT 'trace.logged';
        ALTER TABLE execution_traces ADD COLUMN severity TEXT NOT NULL DEFAULT 'info';
        ALTER TABLE execution_traces ADD COLUMN status TEXT NOT NULL DEFAULT 'recorded';
        ALTER TABLE execution_traces
            ADD COLUMN payload JSONB NOT NULL DEFAULT '{}'::jsonb;
        ALTER TABLE execution_traces
            ADD COLUMN source_refs JSONB NOT NULL DEFAULT '[]'::jsonb;
        ALTER TABLE execution_traces RENAME TO event_journal;
        ALTER INDEX IF EXISTS execution_traces_pkey RENAME TO event_journal_pkey;
        ALTER INDEX IF EXISTS execution_traces_tenant_idx RENAME TO event_journal_tenant_idx;
        ALTER INDEX IF EXISTS execution_traces_agent_idx RENAME TO event_journal_agent_idx;
        ALTER INDEX IF EXISTS execution_traces_session_idx RENAME TO event_journal_session_idx;
        ALTER INDEX IF EXISTS execution_traces_created_idx RENAME TO event_journal_created_idx;
        ALTER INDEX IF EXISTS execution_traces_tenant_created_idx
            RENAME TO event_journal_tenant_created_idx;
        """
    )
