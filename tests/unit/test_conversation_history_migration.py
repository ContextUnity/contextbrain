"""SQLite legacy Conversation History migration and rollback proof."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from contextunity.brain.storage.sqlite import SqliteBrainStore


def _create_legacy_database(path: Path, *, record_id: str) -> None:
    with sqlite3.connect(path) as db:
        db.execute(
            """
            CREATE TABLE episodic_events (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT,
                content TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            INSERT INTO episodic_events
                (id, tenant_id, user_id, session_id, content, metadata, created_at)
            VALUES (?, 'tenant-a', 'user-a', 'session-a', 'hello', '{}',
                    '2026-01-01T00:00:00+00:00')
            """,
            (record_id,),
        )


@pytest.mark.asyncio
async def test_legacy_rows_reconcile_before_source_removal(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    record_id = "11111111-1111-4111-8111-111111111111"
    _create_legacy_database(db_path, record_id=record_id)

    store = SqliteBrainStore(str(db_path))
    records = await store.query_conversation_history(
        tenant_id="tenant-a",
        projection="recent",
        user_id="user-a",
        session_id=None,
        graph_run_id=None,
        older_than_days=None,
        limit=10,
        offset=0,
    )
    assert str(records[0].record_id) == record_id
    assert records[0].kind == "legacy_import"
    with sqlite3.connect(db_path) as db:
        receipt = db.execute(
            """
            SELECT source_count, target_count, source_digest, target_digest
            FROM conversation_migration_receipts WHERE tenant_id = 'tenant-a'
            """
        ).fetchone()
        old_table = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='episodic_events'"
        ).fetchone()
    assert receipt is not None
    assert receipt[0] == receipt[1] == 1
    assert receipt[2] == receipt[3]
    assert old_table is None


def test_malformed_legacy_row_rolls_back_without_data_loss(tmp_path: Path) -> None:
    db_path = tmp_path / "malformed.sqlite3"
    _create_legacy_database(db_path, record_id="not-a-uuid")

    with pytest.raises(sqlite3.IntegrityError, match="malformed legacy conversation"):
        SqliteBrainStore(str(db_path))

    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT count(*) FROM episodic_events").fetchone() == (1,)
        assert (
            db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='conversation_records'"
            ).fetchone()
            is None
        )
