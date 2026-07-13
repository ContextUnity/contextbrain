"""Rename the legacy cells.node_kind column to cell_kind.

Revision ID: 0014_cell_kind_column
Revises: 0013_embedding_jobs
Create Date: 2026-07-13
"""

from collections.abc import Sequence

from alembic import op

revision = "0014_cell_kind_column"
down_revision = "0013_embedding_jobs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Move existing cells to the canonical physical column name."""
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'node_kind'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'cells' AND column_name = 'cell_kind'
            ) THEN
                ALTER TABLE cells RENAME COLUMN node_kind TO cell_kind;
            END IF;
        END $$;
        ALTER INDEX IF EXISTS cells_node_kind_idx RENAME TO cells_cell_kind_idx;
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'cells_node_kind_check'
                  AND conrelid = to_regclass('cells')
            ) THEN
                ALTER TABLE cells RENAME CONSTRAINT cells_node_kind_check TO cells_cell_kind_check;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    """Restore the pre-canonical column name for rollback only."""
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'cells_cell_kind_check'
                  AND conrelid = to_regclass('cells')
            ) THEN
                ALTER TABLE cells RENAME CONSTRAINT cells_cell_kind_check TO cells_node_kind_check;
            END IF;
        END $$;
        ALTER INDEX IF EXISTS cells_cell_kind_idx RENAME TO cells_node_kind_idx;
        ALTER TABLE cells RENAME COLUMN cell_kind TO node_kind;
        """
    )
