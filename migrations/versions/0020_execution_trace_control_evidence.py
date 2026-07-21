"""Persist closed Task-DAG and tool-effect control evidence on execution traces.

Revision ID: 0020_execution_trace_control_evidence
Revises: 0019_udb_mutation_command_digests
Create Date: 2026-07-17
"""

from collections.abc import Sequence

from alembic import op

revision = "0020_execution_trace_control_evidence"
down_revision = "0019_udb_mutation_command_digests"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the bounded JSON object used by terminal Trace receipt projection."""
    op.execute(
        "ALTER TABLE execution_traces "
        "ADD COLUMN IF NOT EXISTS control_evidence JSONB NOT NULL DEFAULT '{}'::jsonb"
    )
    op.execute(
        "ALTER TABLE execution_traces "
        "DROP CONSTRAINT IF EXISTS execution_traces_control_evidence_object_check"
    )
    op.execute(
        "ALTER TABLE execution_traces ADD CONSTRAINT "
        "execution_traces_control_evidence_object_check "
        "CHECK (jsonb_typeof(control_evidence) = 'object')"
    )


def downgrade() -> None:
    """Remove the control-evidence projection column."""
    op.execute(
        "ALTER TABLE execution_traces "
        "DROP CONSTRAINT IF EXISTS execution_traces_control_evidence_object_check"
    )
    op.execute("ALTER TABLE execution_traces DROP COLUMN IF EXISTS control_evidence")
