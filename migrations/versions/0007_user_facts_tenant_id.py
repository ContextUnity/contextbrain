"""Add tenant_id to user_facts and fix primary key.

Revision ID: 0007_user_facts_tenant_id
Revises: 0006_vector_dim_1536
Create Date: 2026-02-13

Adds tenant_id column to user_facts for proper multi-tenant isolation.
Changes PK from (user_id, fact_key) to (tenant_id, user_id, fact_key).
Also adds missing tenant indexes on episodic_events and user_facts.
"""

from alembic import op

# revision identifiers
revision = "0007_user_facts_tenant_id"
down_revision = "0006_vector_dim_1536"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add tenant_id column to user_facts (with default for existing rows)
    op.execute("""
        ALTER TABLE user_facts
        ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';
    """)

    # 2. Drop old PK and recreate with tenant_id
    op.execute("ALTER TABLE user_facts DROP CONSTRAINT IF EXISTS user_facts_pkey;")
    op.execute("""
        ALTER TABLE user_facts
        ADD CONSTRAINT user_facts_pkey PRIMARY KEY (tenant_id, user_id, fact_key);
    """)

    # 3. Add missing indexes
    op.execute("""
        CREATE INDEX IF NOT EXISTS user_facts_tenant_idx ON user_facts (tenant_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS episodic_events_tenant_idx ON episodic_events (tenant_id);
    """)

    # 4. Update upsert_fact ON CONFLICT to use new PK
    # (This is handled in code, not DDL — but we add a partial unique
    #  index for backward compat with old code that may still use
    #  (user_id, fact_key) conflict target)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS user_facts_user_key_uq
        ON user_facts (user_id, fact_key)
        WHERE tenant_id = 'default';
    """)


def downgrade() -> None:
    # Revert PK
    op.execute("ALTER TABLE user_facts DROP CONSTRAINT IF EXISTS user_facts_pkey;")
    op.execute("""
        ALTER TABLE user_facts
        ADD CONSTRAINT user_facts_pkey PRIMARY KEY (user_id, fact_key);
    """)

    # Drop tenant_id column
    op.execute("ALTER TABLE user_facts DROP COLUMN IF EXISTS tenant_id;")

    # Drop indexes
    op.execute("DROP INDEX IF EXISTS user_facts_tenant_idx;")
    op.execute("DROP INDEX IF EXISTS episodic_events_tenant_idx;")
    op.execute("DROP INDEX IF EXISTS user_facts_user_key_uq;")
