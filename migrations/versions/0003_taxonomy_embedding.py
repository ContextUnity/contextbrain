"""Add embedding to catalog_taxonomy for vector search."""

import sqlalchemy as sa
from alembic import op

revision = "0003_taxonomy_embedding"
down_revision = "0002_gardener_pending"
branch_labels = None
depends_on = None

# Default dimension matching knowledge_nodes
VECTOR_DIM = 768


def upgrade() -> None:
    # catalog_taxonomy is created by ContextCommerce — skip if not deployed
    conn = op.get_bind()
    table_exists = conn.execute(
        sa.text("SELECT to_regclass('public.catalog_taxonomy') IS NOT NULL")
    ).scalar()

    if not table_exists:
        print("  ⏭  catalog_taxonomy does not exist — skipping 0003")
        return

    op.execute(f"""
        ALTER TABLE catalog_taxonomy
        ADD COLUMN IF NOT EXISTS embedding VECTOR({VECTOR_DIM});
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_embedding_hnsw
        ON catalog_taxonomy USING hnsw (embedding vector_cosine_ops);
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS catalog_taxonomy_embedding_hnsw;")
    op.execute("ALTER TABLE catalog_taxonomy DROP COLUMN IF EXISTS embedding;")
