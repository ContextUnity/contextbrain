"""Add news_engine tables for news pipeline

Revision ID: news_engine_001
Revises: 
Create Date: 2026-01-28

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'news_engine_001'
down_revision = None  # Set this to the latest migration if needed
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create news_raw table
    op.execute("""
        CREATE TABLE IF NOT EXISTS news_raw (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            url             TEXT NOT NULL,
            headline        TEXT NOT NULL,
            summary         TEXT NOT NULL,
            category        TEXT NULL,
            source_api      TEXT NOT NULL,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            harvested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, url)
        );
    """)
    op.execute("CREATE INDEX IF NOT EXISTS news_raw_tenant_idx ON news_raw (tenant_id);")
    op.execute("CREATE INDEX IF NOT EXISTS news_raw_harvested_idx ON news_raw (harvested_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS news_raw_category_idx ON news_raw (category);")
    
    # Create news_facts table
    op.execute("""
        CREATE TABLE IF NOT EXISTS news_facts (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            url             TEXT NOT NULL,
            headline        TEXT NOT NULL,
            summary         TEXT NOT NULL,
            category        TEXT NULL,
            suggested_agent TEXT NULL,
            significance    DOUBLE PRECISION DEFAULT 0.5,
            atomic_facts    TEXT[] DEFAULT '{}',
            irony_potential TEXT NULL,
            embedding       VECTOR(768) NULL,
            metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
            raw_id          TEXT NULL REFERENCES news_raw(id) ON DELETE SET NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, url)
        );
    """)
    op.execute("CREATE INDEX IF NOT EXISTS news_facts_tenant_idx ON news_facts (tenant_id);")
    op.execute("CREATE INDEX IF NOT EXISTS news_facts_created_idx ON news_facts (created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS news_facts_category_idx ON news_facts (category);")
    op.execute("""
        CREATE INDEX IF NOT EXISTS news_facts_embedding_hnsw
          ON news_facts USING hnsw (embedding vector_cosine_ops);
    """)
    
    # Create news_posts table
    op.execute("""
        CREATE TABLE IF NOT EXISTS news_posts (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            fact_id         TEXT NULL REFERENCES news_facts(id) ON DELETE SET NULL,
            agent           TEXT NOT NULL,
            headline        TEXT NOT NULL,
            content         TEXT NOT NULL,
            emoji           TEXT DEFAULT '📰',
            fact_url        TEXT NULL,
            embedding       VECTOR(768) NULL,
            scheduled_at    TIMESTAMPTZ NULL,
            published_at    TIMESTAMPTZ NULL,
            telegram_msg_id BIGINT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    op.execute("CREATE INDEX IF NOT EXISTS news_posts_tenant_idx ON news_posts (tenant_id);")
    op.execute("CREATE INDEX IF NOT EXISTS news_posts_scheduled_idx ON news_posts (scheduled_at);")
    op.execute("CREATE INDEX IF NOT EXISTS news_posts_published_idx ON news_posts (published_at);")
    op.execute("""
        CREATE INDEX IF NOT EXISTS news_posts_embedding_hnsw
          ON news_posts USING hnsw (embedding vector_cosine_ops);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS news_posts CASCADE;")
    op.execute("DROP TABLE IF EXISTS news_facts CASCADE;")
    op.execute("DROP TABLE IF EXISTS news_raw CASCADE;")
