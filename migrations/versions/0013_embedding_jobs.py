"""Add durable cell embedding enrichment jobs."""

from collections.abc import Sequence

from alembic import op

revision = "0013_embedding_jobs"
down_revision = "0012_remove_user_fact_cell_kind"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the reference-only embedding job ledger and claim index."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cell_embedding_jobs (
            job_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            cell_id TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
            content_hash TEXT NOT NULL,
            profile TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending','processing','ready','failed','skipped')),
            attempt INTEGER NOT NULL DEFAULT 0,
            lease_id TEXT,
            lease_until TIMESTAMPTZ,
            error_code TEXT,
            idempotency_key TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS embedding_jobs_claim_idx "
        "ON cell_embedding_jobs (tenant_id, status, lease_until)"
    )


def downgrade() -> None:
    """Drop the ledger created by this migration."""
    op.execute("DROP TABLE IF EXISTS cell_embedding_jobs")
