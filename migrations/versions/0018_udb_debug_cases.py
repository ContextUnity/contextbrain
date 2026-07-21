"""Create Brain-owned UniversalDebugBus negative-experience tables.

Revision ID: 0018_udb_debug_cases
Revises: 0017_conversation_history
Create Date: 2026-07-16

UDB is deliberately separate from ``execution_traces``: immutable occurrences
correlate into tenant-scoped DebugCase aggregates, and only comparable verified
recovery evidence changes the case score. The runtime schema builder remains the
DDL source of truth; this migration preserves the same durable/RLS contract for
Alembic-managed deployments.
"""

import os
import re

from alembic import op

revision = "0018_udb_debug_cases"
down_revision = "0017_conversation_history"
branch_labels = None
depends_on = None

_TABLES = (
    "debug_cases",
    "debug_case_occurrences",
    "debug_case_recoveries",
    "debug_case_transitions",
    "debug_case_mitigations",
)


def _set_search_path() -> None:
    schema = os.environ.get("BRAIN_SCHEMA") or "brain"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("BRAIN_SCHEMA must be a PostgreSQL identifier")
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    op.execute(f'SET search_path TO "{schema}", public')


def upgrade() -> None:
    """Create UDB tables, database identities, and tenant RLS policies."""
    _set_search_path()
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS debug_cases (
            case_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL CHECK (fingerprint_version = 'contextunity.udb-fingerprint/v1'),
            fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
            fault_class TEXT NOT NULL CHECK (fault_class IN ('agent_fault','infra_fault','upstream_fault','policy_fault','reference_fault')),
            operation_kind TEXT NOT NULL CHECK (operation_kind IN ('brain_search','auto_extract','secure_node','synapse_record','memory_synthesis','embedding_enrichment')),
            policy_version TEXT NOT NULL CHECK (policy_version = 'contextunity.error-evidence/v1'),
            comparison_key JSONB NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('open','resolved')),
            fault_count INTEGER NOT NULL CHECK (fault_count >= 1),
            success_count INTEGER NOT NULL CHECK (success_count >= 0),
            q_error DOUBLE PRECISION NOT NULL CHECK (q_error >= 0.0 AND q_error <= 1.0),
            case_revision INTEGER NOT NULL CHECK (case_revision >= 1),
            first_occurred_at TIMESTAMPTZ NOT NULL,
            last_occurred_at TIMESTAMPTZ NOT NULL,
            resolved_at TIMESTAMPTZ NULL,
            UNIQUE (tenant_id, case_id),
            UNIQUE (tenant_id, fingerprint_version, fingerprint)
        );
        CREATE INDEX IF NOT EXISTS debug_cases_tenant_state_idx ON debug_cases (tenant_id, state, last_occurred_at DESC);
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS debug_case_occurrences (
            occurrence_id UUID PRIMARY KEY,
            case_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            fault_class TEXT NOT NULL,
            operation_kind TEXT NOT NULL,
            fault_code TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            comparison_key JSONB NOT NULL,
            trace_id UUID NULL,
            graph_run_id UUID NULL,
            node_id TEXT NULL,
            step_id UUID NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            canonical_digest TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, case_id, occurrence_id),
            UNIQUE (tenant_id, producer_id, idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS debug_occurrences_tenant_case_idx ON debug_case_occurrences (tenant_id, case_id, occurred_at DESC);
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS debug_case_recoveries (
            recovery_id UUID PRIMARY KEY,
            case_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            comparison_key JSONB NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            exposure_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('verified_recovery_probe','comparable_success')),
            verified_at TIMESTAMPTZ NOT NULL,
            canonical_digest TEXT NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, exposure_id)
        );
        CREATE INDEX IF NOT EXISTS debug_recoveries_tenant_case_idx ON debug_case_recoveries (tenant_id, case_id);
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS debug_case_transitions (
            transition_id TEXT PRIMARY KEY,
            case_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            transition_kind TEXT NOT NULL CHECK (transition_kind IN ('resolved','reopened')),
            expected_case_revision INTEGER NOT NULL,
            trigger_occurrence_id UUID NULL,
            transitioned_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            FOREIGN KEY (tenant_id, case_id, trigger_occurrence_id)
                REFERENCES debug_case_occurrences(tenant_id, case_id, occurrence_id) ON DELETE RESTRICT,
            UNIQUE (case_id, transition_id)
        );
        CREATE INDEX IF NOT EXISTS debug_transitions_tenant_case_idx ON debug_case_transitions (tenant_id, case_id);
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS debug_case_mitigations (
            attempt_id UUID PRIMARY KEY,
            case_id UUID NOT NULL,
            tenant_id TEXT NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('retry','mitigation','manual_probe')),
            idempotency_key TEXT NOT NULL,
            attempted_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, idempotency_key)
        );
        """
    )
    for table in _TABLES:
        policy = f"{table}_tenant_isolation"
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
        op.execute(f"DROP POLICY IF EXISTS {policy} ON {table};")
        op.execute(
            f"""
            CREATE POLICY {policy} ON {table}
            USING (
                tenant_id = current_setting('app.current_tenant', true)
                OR current_setting('app.current_tenant', true) = '*'
            )
            WITH CHECK (
                tenant_id = current_setting('app.current_tenant', true)
                OR current_setting('app.current_tenant', true) = '*'
            );
            """
        )


def downgrade() -> None:
    """Remove the additive UDB tables in reverse dependency order."""
    _set_search_path()
    op.execute("DROP TABLE IF EXISTS debug_case_mitigations;")
    op.execute("DROP TABLE IF EXISTS debug_case_transitions;")
    op.execute("DROP TABLE IF EXISTS debug_case_recoveries;")
    op.execute("DROP TABLE IF EXISTS debug_case_occurrences;")
    op.execute("DROP TABLE IF EXISTS debug_cases;")
