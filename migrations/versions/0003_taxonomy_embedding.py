"""Add embedding to catalog_taxonomy for vector search."""

from alembic import op

revision = "0003_taxonomy_embedding"
down_revision = "0002_gardener_pending"
branch_labels = None
depends_on = None

# Default dimension matching knowledge_nodes
VECTOR_DIM = 768


def upgrade() -> None:
    # Add embedding column using raw SQL (pgvector extension must be enabled)
    op.execute(f"""
        ALTER TABLE catalog_taxonomy
        ADD COLUMN IF NOT EXISTS embedding VECTOR({VECTOR_DIM});
    """)

    # Create HNSW index for vector similarity search
    op.execute("""
        CREATE INDEX IF NOT EXISTS catalog_taxonomy_embedding_hnsw
        ON catalog_taxonomy USING hnsw (embedding vector_cosine_ops);
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS catalog_taxonomy_embedding_hnsw;")
    op.execute("ALTER TABLE catalog_taxonomy DROP COLUMN IF EXISTS embedding;")
