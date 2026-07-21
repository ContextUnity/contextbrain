"""Tests for Brain Admin RPC permission enforcement (WS-8).

Covers:
- All 5 RPCs require admin:read.
- Token with a single allowed_tenants CANNOT AdminSearchTraces / analytics /
  memory-stats without a matching tenant_id.
- admin:all can list all tenants (no tenant_id required).
- Empty allowed_tenants without admin:all → PERMISSION_DENIED.
- _require_admin_tenant_scope: rejects missing tenant_id for non-admin:all tokens.
"""

from __future__ import annotations

import pytest
from contextunity.core import ContextToken
from contextunity.core.permissions import Permissions

from contextunity.brain.service.handlers.admin import (
    AdminHandlersMixin,
    _require_admin_tenant_scope,
)
from contextunity.brain.service.interceptors import RPC_PERMISSION_MAP

# ── helpers ────────────────────────────────────────────────────────────────


def _token(
    permissions: tuple[str, ...],
    allowed_tenants: tuple[str, ...] = (),
) -> ContextToken:
    return ContextToken(
        token_id="test",
        permissions=permissions,
        allowed_tenants=allowed_tenants,
    )


# ── RPC_PERMISSION_MAP completeness ───────────────────────────────────────


class TestAdminRpcsInPermissionMap:
    """All 11 admin RPCs must be present in the Brain interceptor map."""

    ADMIN_RPCS = (
        "ListTenants",
        "AdminSearchTraces",
        "AdminGetTraceDetails",
        "AdminGetSystemAnalytics",
        "AdminGetMemoryLayerStats",
        "AdminGetFilterOptions",
        "AdminGetSessionTraces",
        "AdminGetCells",
        "AdminGetAnalyticsSummary",
    )

    def test_all_admin_rpcs_mapped(self):
        for rpc in self.ADMIN_RPCS:
            assert rpc in RPC_PERMISSION_MAP, f"{rpc} missing from RPC_PERMISSION_MAP"

    def test_all_admin_rpcs_require_admin_read(self):
        for rpc in self.ADMIN_RPCS:
            perm = RPC_PERMISSION_MAP[rpc]
            assert perm == Permissions.ADMIN_READ, f"{rpc} should require admin:read, got {perm!r}"

    def test_admin_rpcs_present_on_servicer(self):
        """Admin RPCs must exist on the generated BrainServiceServicer."""
        from contextunity.core import brain_pb2_grpc

        servicer = brain_pb2_grpc.BrainServiceServicer()
        for rpc in self.ADMIN_RPCS:
            assert hasattr(servicer, rpc), f"Generated servicer missing {rpc}"


# ── _require_admin_tenant_scope ────────────────────────────────────────────


class TestRequireAdminTenantScope:
    """Unit tests for the tenant-scoping helper."""

    def test_admin_all_no_tenant_id_returns_none(self):
        """admin:all + no tenant_id → None (query all tenants)."""
        tok = _token(("admin:all",))
        result = _require_admin_tenant_scope(tok, None, "TestRpc")
        assert result is None

    def test_admin_all_with_tenant_id_returns_tenant(self):
        """admin:all + tenant_id → returns tenant_id as filter."""
        tok = _token(("admin:all",))
        result = _require_admin_tenant_scope(tok, "sample_project", "TestRpc")
        assert result == "sample_project"

    def test_non_admin_all_with_valid_tenant_returns_tenant(self):
        """Single allowed_tenant + matching tenant_id → allowed."""
        tok = _token((Permissions.ADMIN_READ,), ("sample_project",))
        result = _require_admin_tenant_scope(tok, "sample_project", "TestRpc")
        assert result == "sample_project"

    def test_non_admin_all_missing_tenant_id_raises(self):
        """No admin:all + no tenant_id → SecurityError."""
        from contextunity.core.exceptions import SecurityError

        tok = _token((Permissions.ADMIN_READ,), ("sample_project",))
        with pytest.raises(SecurityError, match="tenant_id is required"):
            _require_admin_tenant_scope(tok, None, "AdminSearchTraces")

    def test_empty_allowed_tenants_without_admin_all_raises(self):
        """Empty allowed_tenants + no admin:all → SecurityError (never 'all tenants')."""
        from contextunity.core.exceptions import SecurityError

        tok = _token((Permissions.ADMIN_READ,), ())
        with pytest.raises(SecurityError):
            _require_admin_tenant_scope(tok, "sample_project", "AdminSearchTraces")

    def test_non_admin_all_wrong_tenant_raises(self):
        """Allowed tenant 'sample_project', requesting 'other' → SecurityError."""
        from contextunity.core.exceptions import SecurityError

        tok = _token((Permissions.ADMIN_READ,), ("sample_project",))
        with pytest.raises(SecurityError, match="tenant access denied"):
            _require_admin_tenant_scope(tok, "other", "AdminSearchTraces")

    def test_empty_allowed_tenants_without_admin_all_no_tenant_raises(self):
        """Empty allowed_tenants, no tenant_id → SecurityError (first guard: missing tenant_id)."""
        from contextunity.core.exceptions import SecurityError

        tok = _token((Permissions.ADMIN_READ,), ())
        with pytest.raises(SecurityError):
            _require_admin_tenant_scope(tok, None, "AdminSearchTraces")

    def test_non_context_token_raises(self):
        """Non-ContextToken object → SecurityError."""
        from contextunity.core.exceptions import SecurityError

        with pytest.raises(SecurityError, match="Missing ContextToken"):
            _require_admin_tenant_scope(None, None, "TestRpc")


# ── _token_can_view_tenant (post-fetch scope for by-id lookups) ────────────


class TestTokenCanViewTenant:
    """AdminGetTraceDetails / ListTenants by-id scope check (WS-8 leak fix)."""

    def test_admin_all_can_view_any_tenant(self):
        from contextunity.brain.service.handlers.admin import _token_can_view_tenant

        assert _token_can_view_tenant(_token(("admin:all",)), "other")

    def test_scoped_token_can_view_own_tenant(self):
        from contextunity.brain.service.handlers.admin import _token_can_view_tenant

        assert _token_can_view_tenant(
            _token((Permissions.ADMIN_READ,), ("sample_project",)), "sample_project"
        )

    def test_scoped_token_cannot_view_other_tenant(self):
        """A token scoped to 'sample_project' must NOT view 'other' tenant's trace by id."""
        from contextunity.brain.service.handlers.admin import _token_can_view_tenant

        assert not _token_can_view_tenant(
            _token((Permissions.ADMIN_READ,), ("sample_project",)), "other"
        )

    def test_empty_allowed_tenants_cannot_view_any(self):
        from contextunity.brain.service.handlers.admin import _token_can_view_tenant

        assert not _token_can_view_tenant(_token((Permissions.ADMIN_READ,), ()), "sample_project")

    def test_non_token_cannot_view(self):
        from contextunity.brain.service.handlers.admin import _token_can_view_tenant

        assert not _token_can_view_tenant(None, "sample_project")


# ── AdminHandlersMixin on generated servicer ──────────────────────────────


def test_admin_search_traces_rejects_unbacked_service_filter() -> None:
    from pydantic import ValidationError

    from contextunity.brain.payloads.admin import AdminSearchTracesPayload

    with pytest.raises(ValidationError, match="service filter is unsupported"):
        AdminSearchTracesPayload.model_validate({"tenant_id": "acme", "service": "router"})

    accepted = AdminSearchTracesPayload.model_validate({"tenant_id": "acme", "status": "succeeded"})
    assert accepted.status == "succeeded"
    for invalid in ("", "unknown", "success"):
        with pytest.raises(ValidationError, match="status"):
            AdminSearchTracesPayload.model_validate({"tenant_id": "acme", "status": invalid})


class TestAdminHandlersMixinOnServicer:
    """Verify the mixin methods exist on the BrainServiceServicer surface."""

    ADMIN_METHODS = (
        "ListTenants",
        "AdminSearchTraces",
        "AdminGetTraceDetails",
        "AdminGetSystemAnalytics",
        "AdminGetMemoryLayerStats",
        "AdminGetFilterOptions",
        "AdminGetSessionTraces",
        "AdminGetCells",
        "AdminGetAnalyticsSummary",
    )

    def test_admin_mixin_has_all_methods(self):
        for method in self.ADMIN_METHODS:
            assert hasattr(AdminHandlersMixin, method), (
                f"AdminHandlersMixin missing method {method}"
            )

    def test_brain_service_has_all_admin_methods(self):
        from contextunity.brain.service.brain_service import BrainService

        for method in self.ADMIN_METHODS:
            assert hasattr(BrainService, method), f"BrainService missing admin method {method}"


# ── Permission token semantics ──────────────────────────────────────────────


class TestAdminTokenPermissions:
    """Validate that admin:all expands to include admin:read."""

    def test_admin_all_implies_admin_read(self):
        tok = _token(("admin:all",))
        assert tok.has_permission(Permissions.ADMIN_READ)
        assert tok.has_permission(Permissions.CONVERSATION_READ)

    def test_admin_read_grants_only_bounded_conversation_read(self):
        tok = _token((Permissions.ADMIN_READ,), ("sample_project",))
        assert tok.has_permission(Permissions.CONVERSATION_READ)
        assert not tok.has_permission(Permissions.MEMORY_READ)

    def test_admin_read_alone_does_not_imply_admin_all(self):
        tok = _token(("admin:read",))
        assert not tok.has_permission(Permissions.ADMIN_ALL)

    def test_admin_read_with_single_tenant_cannot_cross_tenant(self):
        """Token with single allowed_tenant cannot access another tenant."""
        tok = _token(("admin:read",), ("sample_project",))
        assert tok.can_access_tenant("sample_project")
        assert not tok.can_access_tenant("other")

    def test_admin_all_can_access_any_tenant(self):
        tok = _token(("admin:all",))
        assert tok.can_access_tenant("sample_project")
        assert tok.can_access_tenant("other")
        assert tok.can_access_tenant("any-tenant")

    def test_empty_allowed_tenants_cannot_access_any_tenant(self):
        """Empty allowed_tenants without admin:all cannot access any tenant."""
        tok = _token(("admin:read",), ())
        assert not tok.can_access_tenant("sample_project")
        assert not tok.can_access_tenant("any-tenant")


# ── RPC_PERMISSION_MAP still covers all servicer methods ─────────────────


class TestRpcPermissionMapComplete:
    """The map must match the full set of BrainServiceServicer methods."""

    def test_map_matches_proto_surface(self):
        from contextunity.core import brain_pb2_grpc

        servicer = brain_pb2_grpc.BrainServiceServicer()
        expected = {
            name
            for name in dir(servicer)
            if not name.startswith("_") and name[:1].isupper() and callable(getattr(servicer, name))
        }
        assert set(RPC_PERMISSION_MAP) == expected, (
            f"Map/servicer mismatch. "
            f"Extra in map: {set(RPC_PERMISSION_MAP) - expected}. "
            f"Missing from map: {expected - set(RPC_PERMISSION_MAP)}"
        )


class TestHoursIntervalIsParameterSafe:
    """Regression: the ``hours`` time filter must use real bound parameters.

    The original handlers used ``INTERVAL '%(hours)s hours'`` — but under
    psycopg3 a placeholder inside a string literal becomes the literal text
    ``$1`` while still binding a parameter, which Postgres rejects at runtime.
    These tests pin the corrected ``make_interval`` form and guard against the
    broken pattern ever returning to the handler source.
    """

    def test_handler_source_has_no_placeholder_inside_interval_literal(self):
        import inspect

        from contextunity.brain.service.handlers import admin

        source = inspect.getsource(admin)
        assert "INTERVAL '%(" not in source, (
            "Found a psycopg placeholder inside a quoted INTERVAL literal — "
            "use make_interval(hours => %(hours)s) instead."
        )

    def test_make_interval_clause_converts_under_psycopg3(self):
        from psycopg._queries import PostgresQuery
        from psycopg.adapt import Transformer

        sql = (
            b"SELECT 1 FROM execution_traces "
            b"WHERE created_at > NOW() - make_interval(hours => %(hours)s)"
        )
        q = PostgresQuery(Transformer())
        q.convert(sql, {"hours": 24})
        # The placeholder must become a real bound parameter ($1), NOT literal
        # text trapped inside a quoted string.
        assert b"$1" in q.query
        assert b"'$1" not in q.query, "placeholder must not be trapped inside a string literal"
        assert q.params and len(q.params) == 1
