"""Phase 3 M01: drop legacy user_facts after empty-state guard.

Revision ID: 0011_drop_user_facts_guard
Revises: 0010_synapses_tenant_q_composite_idx
Create Date: 2026-07-08

Fails closed when ``user_facts`` still holds project data. Only drops the
table when the row count is zero.
"""

from __future__ import annotations

from alembic import op

revision = "0011_drop_user_facts_guard"
down_revision = "0010_synapses_tenant_q_composite_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        DECLARE
            row_count bigint;
        BEGIN
            IF to_regclass('user_facts') IS NULL THEN
                RETURN;
            END IF;
            EXECUTE 'SELECT COUNT(*) FROM user_facts' INTO row_count;
            IF row_count > 0 THEN
                RAISE EXCEPTION
                    'user_facts migration guard: % legacy row(s) exist; migrate to cells first',
                    row_count;
            END IF;
            DROP TABLE user_facts;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_facts (
            tenant_id   TEXT NOT NULL DEFAULT 'default',
            user_id     TEXT NOT NULL,
            fact_key    TEXT NOT NULL,
            fact_value  JSONB NOT NULL,
            confidence  DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            source_id   UUID NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, user_id, fact_key)
        );
        """
    )
