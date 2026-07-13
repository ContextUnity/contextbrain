"""Add tenant_id to user_facts and fix primary key.

Revision ID: 0007_user_facts_tenant_id
Revises: 0006_vector_dim_1536
Create Date: 2026-02-13

Adds tenant_id column to user_facts for proper multi-tenant isolation.
Changes PK from (user_id, fact_key) to (tenant_id, user_id, fact_key).
Also adds missing tenant indexes on episodic_events and user_facts.

Clean-install safe: skips all ``user_facts`` DDL when the legacy table
was never created (Phase 3+ schema no longer includes it).
"""

from alembic import op

revision = "0007_user_facts_tenant_id"
down_revision = "0006_vector_dim_1536"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.user_facts') IS NULL THEN
                RAISE NOTICE 'user_facts absent — skipping 0007 legacy fact DDL';
            ELSE
                ALTER TABLE user_facts
                ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

                ALTER TABLE user_facts DROP CONSTRAINT IF EXISTS user_facts_pkey;
                ALTER TABLE user_facts
                ADD CONSTRAINT user_facts_pkey PRIMARY KEY (tenant_id, user_id, fact_key);

                CREATE INDEX IF NOT EXISTS user_facts_tenant_idx ON user_facts (tenant_id);

                CREATE UNIQUE INDEX IF NOT EXISTS user_facts_user_key_uq
                ON user_facts (user_id, fact_key)
                WHERE tenant_id = 'default';
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.episodic_events') IS NOT NULL THEN
                CREATE INDEX IF NOT EXISTS episodic_events_tenant_idx
                ON episodic_events (tenant_id);
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.user_facts') IS NULL THEN
                RETURN;
            END IF;
            ALTER TABLE user_facts DROP CONSTRAINT IF EXISTS user_facts_pkey;
            ALTER TABLE user_facts
            ADD CONSTRAINT user_facts_pkey PRIMARY KEY (user_id, fact_key);
            ALTER TABLE user_facts DROP COLUMN IF EXISTS tenant_id;
            DROP INDEX IF EXISTS user_facts_tenant_idx;
            DROP INDEX IF EXISTS user_facts_user_key_uq;
        END $$;
        """
    )
    op.execute("DROP INDEX IF EXISTS episodic_events_tenant_idx;")
