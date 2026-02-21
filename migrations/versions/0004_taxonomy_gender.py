"""Add gender to catalog_taxonomy domain constraint.

This migration also merges the news_engine branch.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers
revision = "0004_taxonomy_gender"
down_revision = ("0003_taxonomy_embedding", "news_engine_001")  # Merge heads
branch_labels = None
depends_on = None


def upgrade() -> None:
    # catalog_taxonomy is created by ContextCommerce — skip if not deployed
    conn = op.get_bind()
    table_exists = conn.execute(
        sa.text("SELECT to_regclass('public.catalog_taxonomy') IS NOT NULL")
    ).scalar()

    if not table_exists:
        print("  ⏭  catalog_taxonomy does not exist — skipping 0004")
        return

    # Drop the old constraint and add new one with gender
    op.execute("""
        ALTER TABLE catalog_taxonomy
        DROP CONSTRAINT IF EXISTS catalog_taxonomy_domain_check;
    """)
    op.execute("""
        ALTER TABLE catalog_taxonomy
        ADD CONSTRAINT catalog_taxonomy_domain_check
        CHECK (domain IN (
            'category', 'categories',
            'color', 'colors',
            'size', 'sizes',
            'gender', 'genders'
        ));
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE catalog_taxonomy
        DROP CONSTRAINT IF EXISTS catalog_taxonomy_domain_check;
    """)
    op.execute("""
        ALTER TABLE catalog_taxonomy
        ADD CONSTRAINT catalog_taxonomy_domain_check
        CHECK (domain IN (
            'category', 'categories',
            'color', 'colors',
            'size', 'sizes'
        ));
    """)
