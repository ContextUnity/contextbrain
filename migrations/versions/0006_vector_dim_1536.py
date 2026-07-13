"""Change vector dimension from 768 to 1536 for OpenAI embeddings.

Revision ID: 0006_vector_dim_1536
Revises: 0005_add_search_vector
Create Date: 2026-01-29

OpenAI text-embedding-3-small uses 1536 dimensions.
This migration:
1. Drops existing vector columns and indexes
2. Recreates them with VECTOR(1536)
3. All existing embeddings will be cleared (NULL)

Clean-install safe: skips tables that do not exist; resolves legacy
``knowledge_nodes`` to canonical ``cells`` when needed.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text("SELECT to_regclass(:qualified) IS NOT NULL"),
            {"qualified": f"public.{table_name}"},
        ).scalar()
    )


def _resolve_legacy_or_canonical(legacy: str, canonical: str) -> str | None:
    if _table_exists(legacy):
        return legacy
    if _table_exists(canonical):
        return canonical
    return None


revision = "0006_vector_dim_1536"
down_revision = "0005_add_search_vector"
branch_labels = None
depends_on = None

OLD_DIM = 768
NEW_DIM = 1536

# (legacy_name, canonical_name, legacy_index, canonical_index)
_EMBEDDING_TABLES: tuple[tuple[str, str, str, str], ...] = (
    ("knowledge_nodes", "cells", "knowledge_nodes_embedding_hnsw", "cells_embedding_hnsw"),
    (
        "episodic_events",
        "episodic_events",
        "episodic_events_embedding_hnsw",
        "episodic_events_embedding_hnsw",
    ),
    (
        "catalog_taxonomy",
        "catalog_taxonomy",
        "catalog_taxonomy_embedding_hnsw",
        "catalog_taxonomy_embedding_hnsw",
    ),
    ("news_facts", "news_facts", "news_facts_embedding_hnsw", "news_facts_embedding_hnsw"),
    ("news_posts", "news_posts", "news_posts_embedding_hnsw", "news_posts_embedding_hnsw"),
)


def _embedding_target(
    legacy: str, canonical: str, legacy_idx: str, canonical_idx: str
) -> tuple[str, str] | None:
    table = _resolve_legacy_or_canonical(legacy, canonical)
    if table is None:
        return None
    index_name = legacy_idx if table == legacy else canonical_idx
    return table, index_name


def upgrade() -> None:
    for legacy, canonical, legacy_idx, canonical_idx in _EMBEDDING_TABLES:
        resolved = _embedding_target(legacy, canonical, legacy_idx, canonical_idx)
        if resolved is None:
            print(f"  ⏭  {legacy}/{canonical} does not exist — skipping")
            continue
        table, index_name = resolved

        op.execute(f"DROP INDEX IF EXISTS {index_name};")
        op.execute(
            f"""
            ALTER TABLE {table}
            ALTER COLUMN embedding TYPE VECTOR({NEW_DIM})
            USING NULL::VECTOR({NEW_DIM});
            """
        )
        op.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table} USING hnsw (embedding vector_cosine_ops);
            """
        )


def downgrade() -> None:
    for legacy, canonical, legacy_idx, canonical_idx in _EMBEDDING_TABLES:
        resolved = _embedding_target(legacy, canonical, legacy_idx, canonical_idx)
        if resolved is None:
            continue
        table, index_name = resolved

        op.execute(f"DROP INDEX IF EXISTS {index_name};")
        op.execute(
            f"""
            ALTER TABLE {table}
            ALTER COLUMN embedding TYPE VECTOR({OLD_DIM})
            USING NULL::VECTOR({OLD_DIM});
            """
        )
        op.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table} USING hnsw (embedding vector_cosine_ops);
            """
        )
