"""Tests for legacy user_facts empty-state guard."""

from __future__ import annotations

import sqlite3

import pytest

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.user_facts_guard import guard_and_drop_sqlite_user_facts


def test_guard_drops_empty_user_facts_table(tmp_path):
    db_path = tmp_path / "guard.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE user_facts (
            tenant_id TEXT, user_id TEXT, fact_key TEXT, fact_value TEXT,
            PRIMARY KEY (tenant_id, user_id, fact_key)
        )
        """
    )
    guard_and_drop_sqlite_user_facts(conn)
    conn.commit()
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='user_facts'"
    ).fetchone()
    assert row is None
    conn.close()


def test_guard_fails_closed_when_rows_exist(tmp_path):
    db_path = tmp_path / "guard-fail.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE user_facts (
            tenant_id TEXT, user_id TEXT, fact_key TEXT, fact_value TEXT,
            PRIMARY KEY (tenant_id, user_id, fact_key)
        )
        """
    )
    conn.execute(
        "INSERT INTO user_facts (tenant_id, user_id, fact_key, fact_value) VALUES (?, ?, ?, ?)",
        ("t1", "u1", "lang", "uk"),
    )
    with pytest.raises(BrainValidationError, match="migration guard"):
        guard_and_drop_sqlite_user_facts(conn)
    conn.close()
