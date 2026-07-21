"""Add FinalVerdict projection and immutable OutcomeObservation records.

Revision ID: 0022_outcome_observations
Revises: 0021_execution_trace_artifacts
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision = "0022_outcome_observations"
down_revision = "0021_execution_trace_artifacts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE execution_traces ADD COLUMN IF NOT EXISTS final_verdict JSONB NOT NULL DEFAULT '{}'::jsonb"
    )
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'execution_traces_final_verdict_object_check'
                  AND conrelid = 'execution_traces'::regclass
            ) THEN
                ALTER TABLE execution_traces
                ADD CONSTRAINT execution_traces_final_verdict_object_check
                CHECK (jsonb_typeof(final_verdict) = 'object');
            END IF;
        END $$
    """)
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS execution_traces_tenant_id_uq ON execution_traces (tenant_id, id)"
    )
    op.execute("""
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
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS outcome_observations_trace_idx ON outcome_observations (tenant_id, trace_id)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS outcome_observations_tenant_id_uq ON outcome_observations (tenant_id, observation_id)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS synapses_tenant_id_uq ON synapses (tenant_id, id)"
    )
    op.execute("""
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
            UNIQUE (tenant_id, source_authority, idempotency_key, synapse_id)
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS outcome_synapse_effects_observation_idx ON outcome_synapse_effects (tenant_id, observation_id)"
    )
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'outcome_synapse_effects_synapse_scope_fk'
                  AND conrelid = 'outcome_synapse_effects'::regclass
            ) THEN
                ALTER TABLE outcome_synapse_effects
                ADD CONSTRAINT outcome_synapse_effects_synapse_scope_fk
                FOREIGN KEY (tenant_id, synapse_id)
                REFERENCES synapses(tenant_id, id) ON DELETE RESTRICT;
            END IF;
        END $$
    """)
    op.execute("ALTER TABLE outcome_observations ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE outcome_observations FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE outcome_synapse_effects ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE outcome_synapse_effects FORCE ROW LEVEL SECURITY")
    op.execute(
        "DROP POLICY IF EXISTS outcome_observations_tenant_isolation ON outcome_observations"
    )
    op.execute(
        "DROP POLICY IF EXISTS outcome_synapse_effects_tenant_isolation ON outcome_synapse_effects"
    )
    op.execute("""
        CREATE POLICY outcome_observations_tenant_isolation ON outcome_observations
        USING (tenant_id = current_setting('app.current_tenant', true)
               OR current_setting('app.current_tenant', true) = '*')
        WITH CHECK (tenant_id = current_setting('app.current_tenant', true)
                    OR current_setting('app.current_tenant', true) = '*')
    """)
    op.execute("""
        CREATE POLICY outcome_synapse_effects_tenant_isolation ON outcome_synapse_effects
        USING (tenant_id = current_setting('app.current_tenant', true)
               OR current_setting('app.current_tenant', true) = '*')
        WITH CHECK (tenant_id = current_setting('app.current_tenant', true)
                    OR current_setting('app.current_tenant', true) = '*')
    """)
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_app') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE ON outcome_observations TO brain_app;
                GRANT SELECT, INSERT, UPDATE, DELETE ON outcome_synapse_effects TO brain_app;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'brain_admin') THEN
                GRANT ALL ON outcome_observations TO brain_admin;
                GRANT ALL ON outcome_synapse_effects TO brain_admin;
            END IF;
        END $$
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outcome_synapse_effects")
    op.execute("DROP TABLE IF EXISTS outcome_observations")
    op.execute("DROP INDEX IF EXISTS synapses_tenant_id_uq")
    op.execute("DROP INDEX IF EXISTS execution_traces_tenant_id_uq")
    op.execute(
        "ALTER TABLE execution_traces DROP CONSTRAINT IF EXISTS "
        "execution_traces_final_verdict_object_check"
    )
    op.execute("ALTER TABLE execution_traces DROP COLUMN IF EXISTS final_verdict")
