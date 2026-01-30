"""Change vector dimension from 768 to 1536 for OpenAI embeddings.

Revision ID: 0006_vector_dim_1536
Revises: 0005_add_search_vector
Create Date: 2026-01-29

OpenAI text-embedding-3-small uses 1536 dimensions.
This migration:
1. Drops existing vector columns and indexes
2. Recreates them with VECTOR(1536)
3. All existing embeddings will be cleared (NULL)
"""

from alembic import op


# revision identifiers
revision = "0006_vector_dim_1536"
down_revision = "0005_add_search_vector"
branch_labels = None
depends_on = None

OLD_DIM = 768
NEW_DIM = 1536


def upgrade():
    """Recreate vector columns with 1536 dimensions."""
    
    # Tables with embedding columns
    tables_with_embedding = [
        ("knowledge_nodes", "knowledge_nodes_embedding_hnsw"),
        ("episodic_events", "episodic_events_embedding_hnsw"),
        ("catalog_taxonomy", "catalog_taxonomy_embedding_hnsw"),
        ("news_facts", "news_facts_embedding_hnsw"),
        ("news_posts", "news_posts_embedding_hnsw"),
    ]
    
    for table, index_name in tables_with_embedding:
        # Drop index first
        op.execute(f"DROP INDEX IF EXISTS {index_name};")
        
        # Alter column type
        op.execute(f"""
            ALTER TABLE {table} 
            ALTER COLUMN embedding TYPE VECTOR({NEW_DIM}) 
            USING NULL::VECTOR({NEW_DIM});
        """)
        
        # Recreate index
        op.execute(f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table} USING hnsw (embedding vector_cosine_ops);
        """)


def downgrade():
    """Revert to 768 dimensions."""
    tables_with_embedding = [
        ("knowledge_nodes", "knowledge_nodes_embedding_hnsw"),
        ("episodic_events", "episodic_events_embedding_hnsw"),
        ("catalog_taxonomy", "catalog_taxonomy_embedding_hnsw"),
        ("news_facts", "news_facts_embedding_hnsw"),
        ("news_posts", "news_posts_embedding_hnsw"),
    ]
    
    for table, index_name in tables_with_embedding:
        op.execute(f"DROP INDEX IF EXISTS {index_name};")
        op.execute(f"""
            ALTER TABLE {table} 
            ALTER COLUMN embedding TYPE VECTOR({OLD_DIM}) 
            USING NULL::VECTOR({OLD_DIM});
        """)
        op.execute(f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table} USING hnsw (embedding vector_cosine_ops);
        """)
