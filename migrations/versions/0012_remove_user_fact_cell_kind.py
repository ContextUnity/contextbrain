"""Phase 3: retire legacy ``user_fact`` cell_kind on canonical cells.

Revision ID: 0012_remove_user_fact_cell_kind
Revises: 0011_drop_user_facts_guard
Create Date: 2026-07-08

Rewrites any remaining ``user_fact`` rows to ``fact`` and tightens the
``cells.node_kind`` check constraint.
"""

from __future__ import annotations

from alembic import op

revision = "0012_remove_user_fact_cell_kind"
down_revision = "0011_drop_user_facts_guard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE cells
        SET node_kind = 'fact'
        WHERE node_kind = 'user_fact';

        ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_node_kind_check;
        ALTER TABLE cells ADD CONSTRAINT cells_node_kind_check
            CHECK (node_kind IN (
                'chunk','concept','fact','preference','config','document',
                'documentation','entity','summary'
            ));
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_node_kind_check;
        ALTER TABLE cells ADD CONSTRAINT cells_node_kind_check
            CHECK (node_kind IN (
                'chunk','concept','fact','preference','config','document',
                'documentation','entity','summary','user_fact'
            ));
        """
    )
