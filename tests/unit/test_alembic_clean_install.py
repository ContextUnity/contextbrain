"""Alembic migrations must not assume legacy tables on a fresh install."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_VERSIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"


def _load_migration(name: str):
    path = _VERSIONS / name
    spec = importlib.util.spec_from_file_location(f"_migration_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestAlembicCleanInstallGuards:
    def test_0005_upgrade_sql_is_guarded(self):
        source = (_VERSIONS / "0005_add_search_vector.py").read_text()
        assert "to_regclass('public.knowledge_nodes')" in source
        assert "CREATE INDEX IF NOT EXISTS knowledge_nodes_search_vector_gin" in source
        assert "DO $$" in source

    def test_0007_upgrade_sql_skips_missing_user_facts(self):
        source = (_VERSIONS / "0007_user_facts_tenant_id.py").read_text()
        assert "to_regclass('public.user_facts') IS NULL" in source
        assert "skipping 0007 legacy fact DDL" in source

    def test_0006_module_imports_without_relative_helpers(self):
        module = _load_migration("0006_vector_dim_1536.py")
        assert callable(module.upgrade)
        source = (_VERSIONS / "0006_vector_dim_1536.py").read_text()
        assert "_resolve_legacy_or_canonical" in source
        assert "from ..helpers" not in source

    def test_0011_upgrade_sql_uses_guard(self):
        source = (_VERSIONS / "0011_drop_user_facts_guard.py").read_text()
        assert "to_regclass('user_facts') IS NULL" in source

    def test_0012_migrates_user_fact_cell_kind(self):
        source = (_VERSIONS / "0012_remove_user_fact_cell_kind.py").read_text()
        # Migration 0012 predates the physical column rename in 0014.
        assert "node_kind = 'fact'" in source
        assert "user_fact" in source
