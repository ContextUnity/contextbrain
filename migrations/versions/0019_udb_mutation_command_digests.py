"""Persist immutable command digests for existing UDB mutation receipts.

Revision ID: 0019_udb_mutation_command_digests
Revises: 0018_udb_debug_cases
Create Date: 2026-07-17

``0018_udb_debug_cases`` remains immutable because supported deployments may
already be stamped at that revision. This successor reconstructs digests from
the closed stored command fields before making the columns mandatory.
"""

from __future__ import annotations

import os
import re
from hashlib import sha256
from json import dumps as canonical_dumps

import sqlalchemy as sa
from alembic import op
from contextunity.core.udb import MitigationAttempt, ReopenDebugCase, ResolveDebugCase

revision = "0019_udb_mutation_command_digests"
down_revision = "0018_udb_debug_cases"
branch_labels = None
depends_on = None


def _set_search_path() -> None:
    schema = os.environ.get("BRAIN_SCHEMA") or "brain"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("BRAIN_SCHEMA must be a PostgreSQL identifier")
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    op.execute(f'SET search_path TO "{schema}", public')


def _command_digest(command: MitigationAttempt | ResolveDebugCase | ReopenDebugCase) -> str:
    return sha256(
        canonical_dumps(
            command.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _backfill_mitigations() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT attempt_id, case_id, expected_case_revision, kind,
                   idempotency_key, attempted_at
            FROM debug_case_mitigations
            """
        )
    ).mappings()
    for row in rows:
        command = MitigationAttempt.model_validate(dict(row))
        bind.execute(
            sa.text(
                "UPDATE debug_case_mitigations SET canonical_digest = :digest "
                "WHERE attempt_id = :attempt_id"
            ),
            {"digest": _command_digest(command), "attempt_id": command.attempt_id},
        )


def _backfill_transitions() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT transition_id, case_id, transition_kind, expected_case_revision,
                   trigger_occurrence_id, transitioned_at
            FROM debug_case_transitions
            """
        )
    ).mappings()
    for row in rows:
        values = dict(row)
        transition_id = values.pop("transition_id")
        transition_kind = values.pop("transition_kind")
        transitioned_at = values.pop("transitioned_at")
        trigger_occurrence_id = values.pop("trigger_occurrence_id")
        if transition_kind == "resolved":
            command = ResolveDebugCase.model_validate(
                {**values, "resolution_id": transition_id, "resolved_at": transitioned_at}
            )
        elif transition_kind == "reopened":
            command = ReopenDebugCase.model_validate(
                {
                    **values,
                    "reopen_id": transition_id,
                    "trigger_occurrence_id": trigger_occurrence_id,
                    "reopened_at": transitioned_at,
                }
            )
        else:
            raise ValueError("stored DebugCase transition kind is malformed")
        bind.execute(
            sa.text(
                "UPDATE debug_case_transitions SET canonical_digest = :digest "
                "WHERE transition_id = :transition_id"
            ),
            {"digest": _command_digest(command), "transition_id": transition_id},
        )


def _add_digest_constraint(table: str, constraint: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = '{constraint}'
                  AND conrelid = '{table}'::regclass
            ) THEN
                ALTER TABLE {table}
                    ADD CONSTRAINT {constraint}
                    CHECK (canonical_digest ~ '^[0-9a-f]{{64}}$');
            END IF;
        END $$;
        """
    )


def upgrade() -> None:
    """Backfill and enforce canonical command receipt identity."""
    _set_search_path()
    op.execute("ALTER TABLE debug_case_mitigations ADD COLUMN IF NOT EXISTS canonical_digest TEXT")
    op.execute("ALTER TABLE debug_case_transitions ADD COLUMN IF NOT EXISTS canonical_digest TEXT")
    _backfill_mitigations()
    _backfill_transitions()
    op.execute("ALTER TABLE debug_case_mitigations ALTER COLUMN canonical_digest SET NOT NULL")
    op.execute("ALTER TABLE debug_case_transitions ALTER COLUMN canonical_digest SET NOT NULL")
    _add_digest_constraint(
        "debug_case_mitigations", "debug_case_mitigations_canonical_digest_check"
    )
    _add_digest_constraint(
        "debug_case_transitions", "debug_case_transitions_canonical_digest_check"
    )


def downgrade() -> None:
    """Return to the historical 0018 receipt shape."""
    _set_search_path()
    op.execute(
        "ALTER TABLE debug_case_mitigations "
        "DROP CONSTRAINT IF EXISTS debug_case_mitigations_canonical_digest_check"
    )
    op.execute(
        "ALTER TABLE debug_case_transitions "
        "DROP CONSTRAINT IF EXISTS debug_case_transitions_canonical_digest_check"
    )
    op.execute("ALTER TABLE debug_case_mitigations DROP COLUMN IF EXISTS canonical_digest")
    op.execute("ALTER TABLE debug_case_transitions DROP COLUMN IF EXISTS canonical_digest")
