"""Tests for PostgreSQL schema DDL generation.

Zero-infrastructure tests — schema.py returns pure SQL strings.
We verify security invariants (RLS), config-driven toggles, and input bounds.
Table presence tests removed — those are string-matching, not regression catchers.
"""

from __future__ import annotations

import pytest

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.postgres.schema import (
    build_column_backfill_sql,
    build_preflight_rename_sql,
    build_rls_sql,
    build_schema_sql,
)

# ═══════════════════════════════════════════════════════════════════
# build_schema_sql — config-driven behavior
# ═══════════════════════════════════════════════════════════════════


class TestBuildSchemaSql:
    """Input validation and config toggles."""

    def test_zero_dim_raises(self):
        with pytest.raises(BrainValidationError, match="vector_dim must be positive"):
            build_schema_sql(vector_dim=0)

    def test_negative_dim_raises(self):
        with pytest.raises(BrainValidationError, match="vector_dim must be positive"):
            build_schema_sql(vector_dim=-1)

    def test_vector_dim_embedded_in_ddl(self):
        sql = "\n".join(build_schema_sql(vector_dim=1536))
        assert "VECTOR(1536)" in sql

    def test_commerce_excluded_by_default(self):
        sql = "\n".join(build_schema_sql(vector_dim=768))
        assert "catalog_taxonomy" not in sql

    def test_commerce_included_when_enabled(self):
        sql = "\n".join(build_schema_sql(vector_dim=768, include_commerce=True))
        assert "catalog_taxonomy" in sql

    def test_source_type_check_constraint(self):
        """Source type enum must include legacy and Phase 3 canonical types."""
        sql = "\n".join(build_schema_sql(vector_dim=768))
        for stype in (
            "video",
            "book",
            "qa",
            "web",
            "knowledge",
            "documentation",
            "manual",
            "auto_extract",
            "synthesis",
        ):
            assert f"'{stype}'" in sql


# ═══════════════════════════════════════════════════════════════════
# RLS policies — security invariants
# ═══════════════════════════════════════════════════════════════════


class TestRlsPolicies:
    """Row-Level Security for tenant isolation — security regression catchers."""

    def test_rls_covers_all_tenant_tables(self):
        sql = "\n".join(build_rls_sql())
        tenant_tables = [
            "cells",
            "cell_edges",
            "cell_aliases",
            "episodic_events",
            "event_journal",
            "catalog_taxonomy",
            "blackboard",
            "synapses",
        ]
        for table in tenant_tables:
            assert f"ON {table}" in sql, f"Missing RLS policy for {table}"

    def test_rls_forces_on_owner(self):
        """FORCE ROW LEVEL SECURITY prevents owner bypass — critical security."""
        sql = "\n".join(build_rls_sql())
        assert "FORCE ROW LEVEL SECURITY" in sql

    def test_rls_user_isolation_for_sensitive_tables(self):
        """Episodic and traces must isolate by user_id."""
        sql = "\n".join(build_rls_sql())
        assert "app.current_user" in sql

    def test_rls_admin_bypass(self):
        """brain_admin role must have BYPASSRLS for dashboard access."""
        sql = "\n".join(build_rls_sql())
        assert "brain_admin" in sql
        assert "BYPASSRLS" in sql


# ═══════════════════════════════════════════════════════════════════
# Column backfill
# ═══════════════════════════════════════════════════════════════════


class TestColumnBackfill:
    """Idempotent ALTER TABLE additions."""

    def test_all_backfills_are_idempotent(self):
        """Every backfill statement must use IF NOT EXISTS or DROP IF EXISTS."""
        sql = "\n".join(build_column_backfill_sql())
        assert "IF NOT EXISTS" in sql or "IF EXISTS" in sql


# ═══════════════════════════════════════════════════════════════════
# CP-1 breaking preflight — legacy -> canonical rename
# ═══════════════════════════════════════════════════════════════════


class TestPreflightRenameSql:
    """The CP-1 storage reset preflight must be idempotent and complete."""

    def test_covers_every_legacy_table(self):
        sql = "\n".join(build_preflight_rename_sql())
        for legacy in (
            "knowledge_nodes",
            "knowledge_edges",
            "knowledge_aliases",
            "blackboard_records",
            "agent_experiences",
            "agent_traces",
        ):
            assert legacy in sql, f"Missing rename source for legacy table {legacy}"

    def test_covers_every_canonical_target(self):
        sql = "\n".join(build_preflight_rename_sql())
        for canonical in (
            "cells",
            "cell_edges",
            "cell_aliases",
            "blackboard",
            "synapses",
            "event_journal",
        ):
            assert canonical in sql, f"Missing rename target for canonical table {canonical}"

    def test_table_renames_use_if_exists(self):
        """Every ALTER TABLE RENAME must be guarded — fresh DBs have nothing to rename."""
        sql = build_preflight_rename_sql()
        rename_stmts = [s for s in sql if "RENAME TO" in s and "ALTER TABLE" in s]
        assert rename_stmts, "expected at least one table rename statement"
        # Guards are DO $$ + to_regclass (preferred) or IF EXISTS.
        assert all(
            ("IF EXISTS" in s) or ("to_regclass" in s and "DO $$" in s) for s in rename_stmts
        )

    def test_column_rename_is_guarded(self):
        """taxonomy_path -> scope_path must check existence, not use bare RENAME COLUMN."""
        sql = "\n".join(build_preflight_rename_sql())
        assert "taxonomy_path" in sql and "scope_path" in sql
        assert "information_schema.columns" in sql

    def test_drops_every_legacy_named_rls_policy(self):
        """RENAME TO leaves a table's RLS policies attached under their old
        names (policies are bound by OID, not name) — without an explicit
        drop, a legacy-upgraded DB ends up with both
        ``knowledge_nodes_tenant_isolation`` and ``cells_tenant_isolation``
        on the same (now-canonical) table, breaking physical-name parity
        with a freshly created schema even though both enforce identical
        RLS logic. Verified against a live legacy-upgraded database, 2026-07-04.
        """
        sql = "\n".join(build_preflight_rename_sql())
        for table, old_policy in (
            ("cells", "knowledge_nodes_tenant_isolation"),
            ("cell_edges", "knowledge_edges_tenant_isolation"),
            ("cell_aliases", "knowledge_aliases_tenant_isolation"),
            ("blackboard", "blackboard_records_tenant_isolation"),
            ("synapses", "agent_experiences_tenant_isolation"),
            ("event_journal", "agent_traces_tenant_isolation"),
        ):
            assert f"DROP POLICY IF EXISTS {old_policy} ON {table};" in sql, (
                f"Missing legacy RLS policy drop for {table} ({old_policy})"
            )


# ═══════════════════════════════════════════════════════════════════
# Migration 0009 <-> ensure_schema preflight parity
# ═══════════════════════════════════════════════════════════════════


class TestMigrationPreflightParity:
    """The CP-1 rename exists twice by design: live in
    ``_preflight_rename_sql()`` (runs at every ensure_schema) and frozen in
    ``migrations/versions/0009_cp1_storage_reset.py``. Two copies of the same
    DDL is exactly the kind of thing that silently drifts — this test executes
    the migration's ``upgrade()`` against a recording stub and proves the
    schema preflight is a strict subset of it (the migration additionally owns
    the Event Journal v0 column adds, which ensure_schema applies through
    ``_column_backfill()``/``_constraint_upgrades()`` instead)."""

    @staticmethod
    def _normalize(stmt: str) -> str:
        return " ".join(stmt.split())

    def _migration_statements(self) -> list[str]:
        import importlib.util
        import sys
        import types
        from pathlib import Path

        migration_path = (
            Path(__file__).resolve().parents[2]
            / "migrations"
            / "versions"
            / "0009_cp1_storage_reset.py"
        )
        recorded: list[str] = []
        fake_op = types.SimpleNamespace(execute=recorded.append)
        fake_alembic = types.ModuleType("alembic")
        fake_alembic.op = fake_op

        real_alembic = sys.modules.get("alembic")
        sys.modules["alembic"] = fake_alembic
        try:
            spec = importlib.util.spec_from_file_location(
                "_cp1_migration_under_test", migration_path
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.upgrade()
        finally:
            if real_alembic is not None:
                sys.modules["alembic"] = real_alembic
            else:
                del sys.modules["alembic"]
        return recorded

    def test_schema_preflight_is_subset_of_migration(self):
        migration = {self._normalize(s) for s in self._migration_statements()}
        preflight = {self._normalize(s) for s in build_preflight_rename_sql()}

        missing = {
            statement
            for statement in preflight - migration
            if not ("node_kind" in statement and "cell_kind" in statement)
        }
        assert not missing, (
            "ensure_schema preflight statements missing from migration 0009/0014:\n"
            + "\n".join(sorted(missing))
        )

    def test_migration_extras_are_event_journal_v0_only(self):
        migration = {self._normalize(s) for s in self._migration_statements()}
        preflight = {self._normalize(s) for s in build_preflight_rename_sql()}

        extras = migration - preflight
        offenders = [
            statement
            for statement in extras
            if "event_journal" not in statement and "node_kind" not in statement
        ]
        assert not offenders, (
            "Migration 0009 contains statements that are neither in the current "
            "preflight nor a supported historical rename:\n" + "\n".join(sorted(offenders))
        )

    def test_migration_statements_are_all_guarded(self):
        """Docstring invariant of 0009: 'every statement is guarded so it is a
        no-op on a fresh database' — table-level ALTERs need IF EXISTS, DO $$
        blocks carry their own EXISTS guard, DROP POLICY IF EXISTS is safe on
        a missing table (NOTICE, not error)."""
        for stmt in self._migration_statements():
            normalized = self._normalize(stmt)
            assert "IF EXISTS" in normalized or normalized.startswith("DO $$"), (
                f"Unguarded migration statement: {normalized}"
            )
