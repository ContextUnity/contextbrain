"""Live PostgreSQL proof for the CU-460 Brain table decommission."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, sql
from psycopg.errors import CheckViolation

pytestmark = [pytest.mark.integration_live, pytest.mark.asyncio]

_DSN = (os.environ.get("BRAIN_TEST_DSN") or "").strip()


def _migration_sql() -> tuple[str, str]:
    path = (
        Path(__file__).resolve().parents[2]
        / "migrations"
        / "versions"
        / "0016_decommission_commerce_taxonomy.py"
    )
    statements: list[str] = []
    fake_alembic = types.ModuleType("alembic")
    fake_alembic.op = types.SimpleNamespace(execute=statements.append)
    real_alembic = sys.modules.get("alembic")
    sys.modules["alembic"] = fake_alembic
    try:
        spec = importlib.util.spec_from_file_location("_taxonomy_decommission_live", path)
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
    assert len(statements) == 2
    return statements[0], statements[1]


async def _table_names(conn: AsyncConnection[object]) -> set[str]:
    rows = await (
        await conn.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = current_schema()
            """
        )
    ).fetchall()
    return {str(row[0]) for row in rows}


async def test_upgrade_downgrade_reupgrade() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sql, downgrade_sql = _migration_sql()
    schema_name = f"taxonomy_decommission_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    try:
        _ = await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(schema_name), sql.Identifier(schema_name)
            )
        )
        _ = await conn.execute(
            """
            CREATE TABLE catalog_taxonomy (
                tenant_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                name TEXT NOT NULL,
                path LTREE NOT NULL,
                keywords TEXT[] NOT NULL DEFAULT '{}',
                embedding VECTOR(1536),
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (tenant_id, domain, path)
            );
            CREATE TABLE gardener_pending (
                id SERIAL PRIMARY KEY,
                item_type VARCHAR(50) NOT NULL,
                raw_value TEXT NOT NULL
            );
            """
        )
        _ = await conn.execute(
            """
            INSERT INTO catalog_taxonomy
                (tenant_id, domain, name, path, keywords, metadata)
            VALUES ('tenant-a', 'colors', 'Navy', 'navy', '{}', '{}');
            INSERT INTO gardener_pending (item_type, raw_value)
            VALUES ('colors', 'midnight');
            """
        )
        with pytest.raises(CheckViolation, match="Commerce migration required"):
            _ = await conn.execute(upgrade_sql)
        assert {"catalog_taxonomy", "gardener_pending"} <= await _table_names(conn)
        taxonomy_rows = await (
            await conn.execute("SELECT COUNT(*) FROM catalog_taxonomy")
        ).fetchone()
        gardener_rows = await (
            await conn.execute("SELECT COUNT(*) FROM gardener_pending")
        ).fetchone()
        assert taxonomy_rows == (1,)
        assert gardener_rows == (1,)

        _ = await conn.execute("TRUNCATE catalog_taxonomy, gardener_pending")
        _ = await conn.execute(upgrade_sql)
        assert {"catalog_taxonomy", "gardener_pending"}.isdisjoint(await _table_names(conn))

        _ = await conn.execute(downgrade_sql)
        assert {"catalog_taxonomy", "gardener_pending"} <= await _table_names(conn)

        _ = await conn.execute(upgrade_sql)
        assert {"catalog_taxonomy", "gardener_pending"}.isdisjoint(await _table_names(conn))
    finally:
        _ = await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name))
        )
        await conn.close()
