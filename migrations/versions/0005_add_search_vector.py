"""Add search_vector and keywords_vector columns to knowledge_nodes.

These are TSVECTOR columns for full-text search support.
This migration is idempotent - will not fail if columns already exist.

Revision ID: 0005_add_search_vector
Revises: 0004_taxonomy_gender
Create Date: 2026-01-29
"""

from alembic import op

revision = "0005_add_search_vector"
down_revision = "0004_taxonomy_gender"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add FTS columns on legacy ``knowledge_nodes`` when present.

    Fresh installs created directly with canonical ``cells`` DDL already
    include generated ``search_vector`` / ``keywords_vector`` — skip then.
    """
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.knowledge_nodes') IS NULL THEN
                RAISE NOTICE 'knowledge_nodes absent — skipping 0005 (cells schema is canonical)';
                RETURN;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'knowledge_nodes'
                  AND column_name = 'search_vector'
            ) THEN
                ALTER TABLE knowledge_nodes
                ADD COLUMN search_vector TSVECTOR
                GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'knowledge_nodes'
                  AND column_name = 'keywords_vector'
            ) THEN
                ALTER TABLE knowledge_nodes
                ADD COLUMN keywords_vector TSVECTOR
                GENERATED ALWAYS AS (to_tsvector('simple', COALESCE(keywords_text, ''))) STORED;
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.knowledge_nodes') IS NOT NULL THEN
                CREATE INDEX IF NOT EXISTS knowledge_nodes_search_vector_gin
                  ON knowledge_nodes USING GIN (search_vector);
                CREATE INDEX IF NOT EXISTS knowledge_nodes_keywords_vector_gin
                  ON knowledge_nodes USING GIN (keywords_vector);
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS knowledge_nodes_search_vector_gin;")
    op.execute("DROP INDEX IF EXISTS knowledge_nodes_keywords_vector_gin;")
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.knowledge_nodes') IS NOT NULL THEN
                ALTER TABLE knowledge_nodes DROP COLUMN IF EXISTS search_vector;
                ALTER TABLE knowledge_nodes DROP COLUMN IF EXISTS keywords_vector;
            END IF;
        END $$;
        """
    )
