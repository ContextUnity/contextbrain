"""Add gender to catalog_taxonomy domain constraint.

This migration also merges the news_engine branch.
"""

from alembic import op

# revision identifiers
revision = "0004_taxonomy_gender"
down_revision = ("0003_taxonomy_embedding", "news_engine_001")  # Merge heads
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the old constraint and add new one with gender
    # Support both singular (category) and plural (categories) forms for compatibility
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
