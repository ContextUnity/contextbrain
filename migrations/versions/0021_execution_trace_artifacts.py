"""Add protected per-attempt Execution Trace artifact storage.

Revision ID: 0021_execution_trace_artifacts
Revises: 0020_execution_trace_control_evidence
Create Date: 2026-07-18
"""

from collections.abc import Sequence

from alembic import op

revision = "0021_execution_trace_artifacts"
down_revision = "0020_execution_trace_control_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create ciphertext-only hot artifact storage with tenant RLS."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_trace_artifacts (
            artifact_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            trace_id UUID NOT NULL,
            graph_run_id UUID NOT NULL,
            invocation_id UUID NOT NULL,
            provider_attempt_id UUID NOT NULL,
            artifact_kind TEXT NOT NULL CHECK (artifact_kind = 'model_io'),
            content_schema TEXT NOT NULL CHECK (
                content_schema = 'contextunity.model-io-content/v1'
            ),
            capture_state TEXT NOT NULL CHECK (
                capture_state IN ('captured','disabled','redacted','rejected','unavailable')
            ),
            storage_state TEXT NOT NULL CHECK (
                storage_state IN ('hot','archiving','cold','restoring','purging','purged')
            ),
            lifecycle_profile_id TEXT NOT NULL,
            content_digest TEXT NOT NULL,
            reservation_digest TEXT NOT NULL,
            protected_envelope JSONB NULL,
            archive_receipt JSONB NULL,
            request_bytes BIGINT NOT NULL CHECK (request_bytes >= 0),
            response_bytes BIGINT NOT NULL CHECK (response_bytes >= 0),
            revision BIGINT NOT NULL CHECK (revision >= 1),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            purged_at TIMESTAMPTZ NULL,
            UNIQUE (
                tenant_id, project_id, graph_run_id,
                provider_attempt_id, artifact_kind
            )
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS execution_trace_artifacts_trace_idx "
        "ON execution_trace_artifacts (tenant_id, project_id, trace_id)"
    )
    op.execute("ALTER TABLE execution_trace_artifacts ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE execution_trace_artifacts FORCE ROW LEVEL SECURITY")
    op.execute(
        "DROP POLICY IF EXISTS execution_trace_artifacts_tenant_isolation "
        "ON execution_trace_artifacts"
    )
    op.execute(
        """
        CREATE POLICY execution_trace_artifacts_tenant_isolation
        ON execution_trace_artifacts
        USING (
            tenant_id = current_setting('app.current_tenant', true)
            OR current_setting('app.current_tenant', true) = '*'
        )
        WITH CHECK (
            tenant_id = current_setting('app.current_tenant', true)
            OR current_setting('app.current_tenant', true) = '*'
        )
        """
    )
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON execution_trace_artifacts TO brain_app")


def downgrade() -> None:
    """Remove protected artifact storage without altering immutable traces."""
    op.execute("DROP TABLE IF EXISTS execution_trace_artifacts")
