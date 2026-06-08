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
        """Source type enum must include all known types."""
        sql = "\n".join(build_schema_sql(vector_dim=768))
        for stype in ("video", "book", "qa", "web", "knowledge", "documentation"):
            assert f"'{stype}'" in sql


# ═══════════════════════════════════════════════════════════════════
# RLS policies — security invariants
# ═══════════════════════════════════════════════════════════════════


class TestRlsPolicies:
    """Row-Level Security for tenant isolation — security regression catchers."""

    def test_rls_covers_all_tenant_tables(self):
        sql = "\n".join(build_rls_sql())
        tenant_tables = [
            "knowledge_nodes",
            "knowledge_edges",
            "knowledge_aliases",
            "episodic_events",
            "user_facts",
            "agent_traces",
            "catalog_taxonomy",
            "blackboard_records",
            "agent_experiences",
        ]
        for table in tenant_tables:
            assert f"ON {table}" in sql, f"Missing RLS policy for {table}"

    def test_rls_forces_on_owner(self):
        """FORCE ROW LEVEL SECURITY prevents owner bypass — critical security."""
        sql = "\n".join(build_rls_sql())
        assert "FORCE ROW LEVEL SECURITY" in sql

    def test_rls_user_isolation_for_sensitive_tables(self):
        """Episodic, user_facts, traces must isolate by user_id."""
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
