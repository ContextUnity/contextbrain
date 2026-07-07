"""Add tenant-leading composite index for ranked Synapse lookup.

Revision ID: 0010_synapses_tenant_q_composite_idx
Revises: 0009_cp1_storage_reset
Create Date: 2026-07-06

``query_synapses`` always scopes by ``tenant_id`` and orders by
``q_composite DESC``. With only the single-column ``synapses_q_composite_idx``
Postgres scans the global q_composite index and filters out every other
tenant's rows (read amplification that grows with total row count — measured
removing 12k+ rows to return 10). The tenant-leading composite index lets the
planner seek straight to the tenant's slice and walk it pre-sorted. Purely
additive and idempotent; ``storage/postgres/schema.py`` is the live source of
truth this mirrors.
"""

from alembic import op

# revision identifiers
revision = "0010_synapses_tenant_q_composite_idx"
down_revision = "0009_cp1_storage_reset"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS synapses_tenant_q_composite_idx "
        "ON synapses (tenant_id, q_composite DESC);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS synapses_tenant_q_composite_idx;")
