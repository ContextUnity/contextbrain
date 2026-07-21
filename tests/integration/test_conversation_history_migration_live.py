"""Live PostgreSQL upgrade, downgrade, re-upgrade, and rejection proof."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, errors, sql

from contextunity.brain.storage.postgres import PostgresBrainStore

pytestmark = [pytest.mark.integration_live, pytest.mark.asyncio]
_DSN = (os.environ.get("BRAIN_TEST_DSN") or "").strip()


def _migration_operations() -> tuple[str, str]:
    path = (
        Path(__file__).resolve().parents[2]
        / "migrations"
        / "versions"
        / "0017_conversation_history.py"
    )
    recorded: list[str] = []
    fake_alembic = types.ModuleType("alembic")
    fake_alembic.op = types.SimpleNamespace(execute=recorded.append)
    real_alembic = sys.modules.get("alembic")
    sys.modules["alembic"] = fake_alembic
    try:
        spec = importlib.util.spec_from_file_location("_conversation_migration_live", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.upgrade()
        module.downgrade()
    finally:
        if real_alembic is None:
            del sys.modules["alembic"]
        else:
            sys.modules["alembic"] = real_alembic
    assert len(recorded) == 2
    return recorded[0], recorded[1]


async def _create_legacy_table(conn: AsyncConnection[object]) -> None:
    await conn.execute(
        """
        CREATE TABLE episodic_events (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_id TEXT,
            content TEXT NOT NULL,
            embedding VECTOR(1536),
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


async def test_upgrade_downgrade_reupgrade_preserves_identity_and_receipt() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sql, downgrade_sql = _migration_operations()
    schema = f"conversation_migration_{uuid4().hex}"
    record_id = uuid4()
    modern_record_id = uuid4()
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    try:
        await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(schema), sql.Identifier(schema)
            )
        )
        await _create_legacy_table(conn)
        await conn.execute(
            """
            INSERT INTO episodic_events
                (id, tenant_id, user_id, session_id, content, metadata)
            VALUES (%s, 'tenant-a', 'user-a', 'session-a', 'hello', '{}'::jsonb)
            """,
            (record_id,),
        )
        await conn.execute(upgrade_sql)
        canonical = await (
            await conn.execute(
                """
                SELECT record_id, kind, idempotency_key
                FROM conversation_records WHERE tenant_id = 'tenant-a'
                """
            )
        ).fetchone()
        receipt = await (
            await conn.execute(
                """
                SELECT source_count, target_count, source_digest, target_digest
                FROM conversation_migration_receipts WHERE tenant_id = 'tenant-a'
                """
            )
        ).fetchone()
        assert canonical == (record_id, "legacy_import", f"legacy:{record_id}")
        assert receipt is not None and receipt[0] == receipt[1] == 1
        assert receipt[2] == receipt[3]

        await conn.execute(
            """
            INSERT INTO conversation_records (
                record_id, tenant_id, user_id, session_id, role, kind, content,
                content_hash, source_hash, graph_run_id, metadata_version,
                idempotency_key, metadata
            ) VALUES (
                %s, 'tenant-a', 'user-a', 'session-a', 'assistant',
                'conversation_note', 'modern',
                'sha256:' || encode(sha256(convert_to('modern', 'UTF8')), 'hex'),
                'sha256:' || encode(sha256(convert_to('source', 'UTF8')), 'hex'),
                NULL, 1, 'modern-key', '{"origin":"router"}'::jsonb
            )
            """,
            (modern_record_id,),
        )

        await conn.execute(downgrade_sql)
        await conn.execute(upgrade_sql)
        replayed = await (
            await conn.execute(
                """
                SELECT record_id, role, kind, idempotency_key, metadata
                FROM conversation_records ORDER BY record_id
                """
            )
        ).fetchall()
        by_id = {row[0]: row[1:] for row in replayed}
        assert by_id[record_id][:3] == (
            "legacy",
            "legacy_import",
            f"legacy:{record_id}",
        )
        assert by_id[modern_record_id][:3] == (
            "assistant",
            "conversation_note",
            "modern-key",
        )
        assert by_id[modern_record_id][3] == {"origin": "router"}
        replay_receipt = await (
            await conn.execute(
                """
                SELECT source_count, target_count, source_digest, target_digest
                FROM conversation_migration_receipts WHERE tenant_id = 'tenant-a'
                """
            )
        ).fetchone()
        assert replay_receipt is not None
        assert replay_receipt[0] == replay_receipt[1] == 2
        assert replay_receipt[2] == replay_receipt[3]
    finally:
        await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        await conn.close()


async def test_malformed_row_rolls_back_and_preserves_source() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sql, _ = _migration_operations()
    schema = f"conversation_reject_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    try:
        await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(schema), sql.Identifier(schema)
            )
        )
        await _create_legacy_table(conn)
        await conn.execute(
            """
            INSERT INTO episodic_events (id, tenant_id, user_id, content, metadata)
            VALUES (%s, '', 'user-a', 'hello', '{}'::jsonb)
            """,
            (uuid4(),),
        )
        with pytest.raises(errors.CheckViolation, match="malformed legacy conversation"):
            await conn.execute(upgrade_sql)
        assert await (await conn.execute("SELECT count(*) FROM episodic_events")).fetchone() == (1,)
    finally:
        await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        await conn.close()


async def test_startup_preflight_rejects_malformed_row_before_rename() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    schema = f"conversation_startup_reject_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    store = PostgresBrainStore(dsn=_DSN, schema=schema, pool_min_size=1, pool_max_size=1)
    try:
        await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        await conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        await _create_legacy_table(conn)
        await conn.execute(
            """
            INSERT INTO episodic_events (id, tenant_id, user_id, content, metadata)
            VALUES (%s, 'tenant-a', '', 'hello', '{}'::jsonb)
            """,
            (uuid4(),),
        )

        with pytest.raises(errors.CheckViolation, match="malformed legacy conversation"):
            await store.ensure_schema()

        source = await (await conn.execute("SELECT count(*) FROM episodic_events")).fetchone()
        canonical = await (
            await conn.execute("SELECT to_regclass('conversation_records')")
        ).fetchone()
        assert source == (1,)
        assert canonical == (None,)
    finally:
        await store.close()
        await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        await conn.close()
