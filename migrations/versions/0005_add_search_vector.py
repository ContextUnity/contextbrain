"""Add search_vector and keywords_vector columns to knowledge_nodes.

These are TSVECTOR columns for full-text search support.
This migration is idempotent - will not fail if columns already exist.

Revision ID: 0005_add_search_vector
Revises: 0004_taxonomy_gender
Create Date: 2026-01-29
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005_add_search_vector"
down_revision = "0004_taxonomy_gender"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add search_vector and keywords_vector columns if they don't exist."""
    
    # Check if columns exist and add them if not
    # PostgreSQL 12+ supports "IF NOT EXISTS" for columns indirectly via DO block
    
    op.execute("""
        DO $$
        BEGIN
            -- Add search_vector column if it doesn't exist
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'knowledge_nodes' AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE knowledge_nodes 
                ADD COLUMN search_vector TSVECTOR 
                GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED;
                
                RAISE NOTICE 'Added search_vector column';
            END IF;
            
            -- Add keywords_vector column if it doesn't exist
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'knowledge_nodes' AND column_name = 'keywords_vector'
            ) THEN
                ALTER TABLE knowledge_nodes 
                ADD COLUMN keywords_vector TSVECTOR 
                GENERATED ALWAYS AS (to_tsvector('simple', COALESCE(keywords_text, ''))) STORED;
                
                RAISE NOTICE 'Added keywords_vector column';
            END IF;
        END $$;
    """)
    
    # Create GIN indexes for full-text search
    op.execute("""
        CREATE INDEX IF NOT EXISTS knowledge_nodes_search_vector_gin
          ON knowledge_nodes USING GIN (search_vector);
    """)
    
    op.execute("""
        CREATE INDEX IF NOT EXISTS knowledge_nodes_keywords_vector_gin
          ON knowledge_nodes USING GIN (keywords_vector);
    """)


def downgrade() -> None:
    """Remove search_vector and keywords_vector columns."""
    op.execute("DROP INDEX IF EXISTS knowledge_nodes_search_vector_gin;")
    op.execute("DROP INDEX IF EXISTS knowledge_nodes_keywords_vector_gin;")
    op.execute("ALTER TABLE knowledge_nodes DROP COLUMN IF EXISTS search_vector;")
    op.execute("ALTER TABLE knowledge_nodes DROP COLUMN IF EXISTS keywords_vector;")
