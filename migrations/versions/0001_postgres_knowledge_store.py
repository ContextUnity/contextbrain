"""Create knowledge store tables."""

from __future__ import annotations

from alembic import op

from contextunity.brain.core.config import get_env
from contextunity.brain.storage.postgres.schema import build_schema_sql

# revision identifiers, used by Alembic.
revision = "0001_postgres_knowledge_store"
down_revision = None
branch_labels = None
depends_on = None


def _is_post_0001_udb_ddl(statement: str) -> bool:
    """Keep UDB in its own migration instead of leaking live DDL into 0001."""
    return "debug_case" in statement or "debug_" in statement


def upgrade() -> None:
    vector_dim = int(get_env("PGVECTOR_DIM") or 768)
    for stmt in build_schema_sql(vector_dim=vector_dim):
        if not _is_post_0001_udb_ddl(stmt):
            op.execute(stmt)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS knowledge_aliases;")
    op.execute("DROP TABLE IF EXISTS knowledge_edges;")
    op.execute("DROP TABLE IF EXISTS knowledge_nodes;")
