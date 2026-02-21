"""Tests for Row-Level Security (RLS) policies.

Verifies:
- All tenant-scoped tables have RLS policies defined
- set_tenant_context() is fail-closed (empty tenant_id → ValueError)
- RLS policy SQL contains wildcard support for admin access
- brain_app and brain_admin roles are created
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

# ── RLS Schema Tests ─────────────────────────────────────────────


class TestRLSPolicies:
    """Verify RLS policy SQL generation."""

    EXPECTED_TENANT_TABLES = [
        "knowledge_nodes",
        "knowledge_edges",
        "knowledge_aliases",
        "episodic_events",
        "user_facts",
        "agent_traces",
        "catalog_taxonomy",
        "news_raw",
        "news_facts",
        "news_posts",
    ]

    def test_rls_policies_cover_all_tenant_tables(self):
        """Step 6.4: RLS policies cover all tenant-scoped tables."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        for table in self.EXPECTED_TENANT_TABLES:
            assert "ENABLE ROW LEVEL SECURITY" in sql_text
            assert f"{table}_tenant_isolation" in sql_text, f"Missing RLS policy for table: {table}"

    def test_rls_policies_have_wildcard_for_admin(self):
        """Admin dashboard can see all projects via wildcard '*'."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        assert "= '*'" in sql_text, "RLS policies must include wildcard '*' for admin access"

    def test_rls_creates_brain_app_role(self):
        """brain_app role (non-superuser, RLS enforced) is created."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        assert "brain_app" in sql_text
        assert "NOLOGIN" in sql_text

    def test_rls_creates_brain_admin_role(self):
        """brain_admin role (BYPASSRLS for dashboard) is created."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        assert "brain_admin" in sql_text
        assert "BYPASSRLS" in sql_text

    def test_rls_force_rls_on_all_tables(self):
        """FORCE ROW LEVEL SECURITY ensures RLS applies to table owner too."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        for table in self.EXPECTED_TENANT_TABLES:
            assert f"ALTER TABLE IF EXISTS {table} FORCE ROW LEVEL SECURITY" in sql_text, (
                f"Missing FORCE RLS for table: {table}"
            )

    def test_rls_grants_to_brain_app(self):
        """brain_app gets SELECT/INSERT/UPDATE/DELETE on tenant tables."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        for table in self.EXPECTED_TENANT_TABLES:
            assert f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO brain_app" in sql_text

    def test_rls_grants_all_to_brain_admin(self):
        """brain_admin gets ALL on tenant tables (with BYPASSRLS)."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        for table in self.EXPECTED_TENANT_TABLES:
            assert f"GRANT ALL ON {table} TO brain_admin" in sql_text

    def test_rls_policy_uses_current_setting(self):
        """RLS USING clause references app.current_tenant session variable."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        assert "current_setting('app.current_tenant'" in sql_text

    def test_rls_policy_with_check_clause(self):
        """RLS WITH CHECK clause prevents cross-tenant INSERT/UPDATE."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts = build_rls_sql()
        sql_text = "\n".join(stmts)

        assert "WITH CHECK" in sql_text

    def test_rls_statements_are_idempotent(self):
        """Running build_rls_sql() twice produces identical statements."""
        from contextbrain.storage.postgres.schema import build_rls_sql

        stmts1 = build_rls_sql()
        stmts2 = build_rls_sql()
        assert stmts1 == stmts2

    def test_expected_table_count(self):
        """Exactly 10 tenant-scoped tables are configured."""
        assert len(self.EXPECTED_TENANT_TABLES) == 10


# ── set_tenant_context Tests ─────────────────────────────────────


class TestSetTenantContext:
    """Verify fail-closed behavior for tenant context setting."""

    @pytest.mark.asyncio
    async def test_empty_tenant_id_raises_value_error(self):
        """Fail-closed: empty tenant_id → ValueError."""
        from contextbrain.storage.postgres.store.helpers import set_tenant_context

        mock_conn = AsyncMock()
        with pytest.raises(ValueError, match="tenant_id"):
            await set_tenant_context(mock_conn, "")

    @pytest.mark.asyncio
    async def test_none_tenant_id_raises_value_error(self):
        """Fail-closed: None tenant_id → ValueError."""
        from contextbrain.storage.postgres.store.helpers import set_tenant_context

        mock_conn = AsyncMock()
        with pytest.raises((ValueError, TypeError)):
            await set_tenant_context(mock_conn, None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_valid_tenant_id_sets_config(self):
        """Valid tenant_id calls SET on the connection."""
        from contextbrain.storage.postgres.store.helpers import set_tenant_context

        mock_conn = AsyncMock()
        await set_tenant_context(mock_conn, "project_a")

        # Should have called execute with SET LOCAL
        mock_conn.execute.assert_called_once()
        call_args = str(mock_conn.execute.call_args)
        assert "app.current_tenant" in call_args or "project_a" in call_args

    @pytest.mark.asyncio
    async def test_wildcard_tenant_id_for_admin(self):
        """Wildcard '*' is accepted for admin/dashboard access."""
        from contextbrain.storage.postgres.store.helpers import set_tenant_context

        mock_conn = AsyncMock()
        await set_tenant_context(mock_conn, "*")
        mock_conn.execute.assert_called_once()
