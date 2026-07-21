"""Regression coverage for CU-460 Brain surface decommission."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
import types
from pathlib import Path

import pytest

from contextunity.brain.storage.postgres.schema import build_rls_sql, build_schema_sql
from contextunity.brain.storage.sqlite.schema import apply_preflight_renames, build_core_ddl


def test_removed_contract_names_have_no_runtime_source() -> None:
    root = Path(__file__).resolve().parents[4]
    source_roots = (
        root / "packages" / "core" / "protos",
        root / "packages" / "core" / "src",
        root / "services" / "brain" / "src",
    )
    removed_names = (
        "UpsertTaxonomy",
        "GetTaxonomy",
        "GetPendingVerifications",
        "SubmitVerification",
        "GetPendingPayload",
        "SubmitVerificationPayload",
        "include_commerce",
    )
    residuals: list[str] = []
    for source_root in source_roots:
        for path in source_root.rglob("*"):
            if path.suffix not in {".py", ".proto", ".pyi"}:
                continue
            content = path.read_text(encoding="utf-8")
            for removed_name in removed_names:
                if removed_name in content:
                    residuals.append(f"{path.relative_to(root)}:{removed_name}")
    assert residuals == []


def _migration_sql() -> tuple[str, str]:
    migration_path = (
        Path(__file__).resolve().parents[2]
        / "migrations"
        / "versions"
        / "0016_decommission_commerce_taxonomy.py"
    )
    statements: list[str] = []
    fake_op = types.SimpleNamespace(execute=statements.append)
    fake_alembic = types.ModuleType("alembic")
    fake_alembic.op = fake_op
    real_alembic = sys.modules.get("alembic")
    sys.modules["alembic"] = fake_alembic
    try:
        spec = importlib.util.spec_from_file_location("_taxonomy_decommission", migration_path)
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


def test_fresh_schemas_and_rls_exclude_legacy_commerce_tables() -> None:
    postgres_sql = "\n".join(build_schema_sql(vector_dim=1536))
    rls_sql = "\n".join(build_rls_sql())
    sqlite_sql = "\n".join(build_core_ddl())
    for legacy_name in ("catalog_taxonomy", "gardener_pending"):
        assert legacy_name not in postgres_sql
        assert legacy_name not in rls_sql
        assert legacy_name not in sqlite_sql


def test_sqlite_existing_schema_drops_legacy_commerce_tables() -> None:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE catalog_taxonomy (tenant_id TEXT NOT NULL)")
    db.execute("CREATE TABLE gardener_pending (id INTEGER PRIMARY KEY)")

    apply_preflight_renames(db)

    remaining = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    assert "catalog_taxonomy" not in remaining
    assert "gardener_pending" not in remaining


def test_sqlite_existing_schema_rejects_unmigrated_commerce_rows() -> None:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE catalog_taxonomy (tenant_id TEXT NOT NULL)")
    db.execute("INSERT INTO catalog_taxonomy (tenant_id) VALUES ('tenant-a')")

    with pytest.raises(sqlite3.IntegrityError, match="Commerce migration required"):
        apply_preflight_renames(db)

    remaining = db.execute("SELECT 1 FROM catalog_taxonomy WHERE tenant_id = 'tenant-a'").fetchone()
    assert remaining == (1,)


def test_postgres_migration_is_fail_closed_and_schema_reversible() -> None:
    upgrade_sql, downgrade_sql = _migration_sql()
    guard_position = upgrade_sql.index("DO $$")
    drop_position = upgrade_sql.index("DROP TABLE IF EXISTS catalog_taxonomy")
    assert guard_position < drop_position
    assert "SELECT EXISTS (SELECT 1 FROM catalog_taxonomy LIMIT 1)" in upgrade_sql
    assert "SELECT EXISTS (SELECT 1 FROM gardener_pending LIMIT 1)" in upgrade_sql
    assert "Commerce migration required" in upgrade_sql
    assert "ERRCODE = '23514'" in upgrade_sql
    assert "DROP TABLE IF EXISTS catalog_taxonomy" in upgrade_sql
    assert "DROP TABLE IF EXISTS gardener_pending" in upgrade_sql
    assert "CREATE TABLE IF NOT EXISTS catalog_taxonomy" in downgrade_sql
    assert "CREATE TABLE IF NOT EXISTS gardener_pending" in downgrade_sql
    assert "catalog_taxonomy_tenant_isolation" in downgrade_sql
