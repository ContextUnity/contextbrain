"""Retire legacy ``user_fact`` values from the canonical cells kind column.

Revision ID: 0012_remove_user_fact_cell_kind
Revises: 0011_drop_user_facts_guard
Create Date: 2026-07-08

Some historical databases reached this revision with ``node_kind`` while a
clean current base already uses ``cell_kind``. The migration recognizes either
physical column and only tightens the matching constraint.
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
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'node_kind'
            ) THEN
                UPDATE cells SET node_kind = 'fact' WHERE node_kind = 'user_fact';
                ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_node_kind_check;
                ALTER TABLE cells ADD CONSTRAINT cells_node_kind_check
                    CHECK (node_kind IN (
                        'chunk','concept','fact','preference','config','document',
                        'documentation','entity','summary'
                    ));
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'cell_kind'
            ) THEN
                UPDATE cells SET cell_kind = 'fact' WHERE cell_kind = 'user_fact';
                ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_cell_kind_check;
                ALTER TABLE cells ADD CONSTRAINT cells_cell_kind_check
                    CHECK (cell_kind IN (
                        'chunk','concept','fact','preference','config','document',
                        'documentation','entity','summary'
                    ));
            ELSE
                RAISE EXCEPTION 'cells kind column is missing';
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'node_kind'
            ) THEN
                ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_node_kind_check;
                ALTER TABLE cells ADD CONSTRAINT cells_node_kind_check
                    CHECK (node_kind IN (
                        'chunk','concept','fact','preference','config','document',
                        'documentation','entity','summary','user_fact'
                    ));
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'cell_kind'
            ) THEN
                ALTER TABLE cells DROP CONSTRAINT IF EXISTS cells_cell_kind_check;
                ALTER TABLE cells ADD CONSTRAINT cells_cell_kind_check
                    CHECK (cell_kind IN (
                        'chunk','concept','fact','preference','config','document',
                        'documentation','entity','summary','user_fact'
                    ));
            ELSE
                RAISE EXCEPTION 'cells kind column is missing';
            END IF;
        END $$;
        """
    )
