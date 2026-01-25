"""Create gardener pending table."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002_gardener_pending"
down_revision = "0001_postgres_knowledge_store"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gardener_pending",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("item_type", sa.String(50), nullable=False),  # category, size, color
        sa.Column("raw_value", sa.Text, nullable=False),
        sa.Column("context", JSONB, default={}),
        sa.Column("proposal", sa.Text, nullable=True),  # Suggestion by LLM
        sa.Column("status", sa.String(20), default="pending"),  # pending, approved, rejected
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, onupdate=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("gardener_pending")
