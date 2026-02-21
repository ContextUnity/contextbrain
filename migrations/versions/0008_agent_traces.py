"""Create agent_traces table.

Revision ID: 0008_agent_traces
Revises: 0007_user_facts_tenant_id
Create Date: 2026-02-14

Stores agent execution traces for observability, debugging, and audit.
Each trace captures: agent identity, graph used, tool calls, token usage,
timing, and security flags from Shield.
"""

from alembic import op

# revision identifiers
revision = "0008_agent_traces"
down_revision = "0007_user_facts_tenant_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS agent_traces (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id   TEXT NOT NULL,
            agent_id    TEXT NOT NULL,
            session_id  TEXT NULL,
            user_id     TEXT NULL,
            graph_name  TEXT NULL,

            -- Execution data (stored as JSONB for flexibility)
            tool_calls  JSONB NOT NULL DEFAULT '[]'::jsonb,
            token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,
            timing_ms   INTEGER NULL,

            -- Security audit
            security_flags JSONB NOT NULL DEFAULT '{}'::jsonb,

            -- Extensible metadata
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,

            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # Indexes for common queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS agent_traces_tenant_idx
        ON agent_traces (tenant_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS agent_traces_agent_idx
        ON agent_traces (agent_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS agent_traces_session_idx
        ON agent_traces (session_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS agent_traces_created_idx
        ON agent_traces (created_at DESC);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS agent_traces_tenant_created_idx
        ON agent_traces (tenant_id, created_at DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS agent_traces;")
