"""Tests for Brain gRPC service helpers.

Zero-infrastructure tests for tenant resolution, user access validation,
and response construction.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from contextunity.core.exceptions import SecurityError

from contextunity.brain.service.helpers import (
    resolve_tenant_id,
    validate_tenant_access,
    validate_user_access,
)

# ── Fixtures ──────────────────────────────────────────────────────


def _token(
    *,
    tenants: list[str] | None = None,
    user_id: str | None = None,
    expired: bool = False,
) -> SimpleNamespace:
    """Lightweight token stub."""
    t = SimpleNamespace()
    t.allowed_tenants = tenants
    t.user_id = user_id
    t.is_expired = lambda: expired
    t.permissions = ("brain:read", "brain:write")
    tenant_set = tuple(tenants or ())
    t.can_access_tenant = lambda tid: tid in tenant_set
    return t


def _context():
    return MagicMock()


# ═══════════════════════════════════════════════════════════════════
# resolve_tenant_id
# ═══════════════════════════════════════════════════════════════════


class TestResolveTenantId:
    """SPOT rule: token is source of truth for tenant."""

    def test_token_tenant_preferred(self):
        token = _token(tenants=["acme"])
        assert resolve_tenant_id(token) == "acme"

    def test_payload_tenant_matches_token(self):
        token = _token(tenants=["acme", "corp"])
        assert resolve_tenant_id(token, "corp") == "corp"

    def test_payload_tenant_not_in_token_uses_first(self):
        token = _token(tenants=["acme"])
        assert resolve_tenant_id(token, "other") == "acme"

    def test_no_token_uses_payload(self):
        assert resolve_tenant_id(None, "legacy") == "legacy"

    def test_no_token_no_payload_defaults(self):
        assert resolve_tenant_id(None) == "default"

    def test_token_without_tenants_uses_payload(self):
        token = _token(tenants=None)
        assert resolve_tenant_id(token, "fallback") == "fallback"

    def test_token_with_empty_tenants_uses_payload(self):
        token = _token(tenants=[])
        assert resolve_tenant_id(token, "legacy") == "legacy"


# ═══════════════════════════════════════════════════════════════════
# validate_tenant_access
# ═══════════════════════════════════════════════════════════════════


class TestValidateTenantAccess:
    """Tenant boundary enforcement."""

    def test_no_token_passes(self):
        """Missing token handled elsewhere."""
        validate_tenant_access(None, "acme", _context())

    def test_empty_tenant_passes(self):
        """Token-only mode — no payload tenant."""
        validate_tenant_access(_token(tenants=["acme"]), "", _context())

    def test_matching_tenant_passes(self):
        validate_tenant_access(_token(tenants=["acme"]), "acme", _context())

    def test_cross_tenant_rejected(self):
        with pytest.raises(SecurityError, match="Tenant access denied"):
            validate_tenant_access(_token(tenants=["acme"]), "evil", _context())

    def test_empty_token_scope_rejects_payload_tenant(self):
        with pytest.raises(SecurityError, match="Tenant access denied"):
            validate_tenant_access(_token(tenants=[]), "acme", _context())


# ═══════════════════════════════════════════════════════════════════
# validate_user_access
# ═══════════════════════════════════════════════════════════════════


class TestValidateUserAccess:
    """User boundary enforcement."""

    def test_no_token_passes(self):
        validate_user_access(None, "user1", _context())

    def test_matching_user_passes(self):
        validate_user_access(_token(user_id="user1"), "user1", _context())

    def test_cross_user_rejected(self):
        with pytest.raises(SecurityError, match="Cross-user access denied"):
            validate_user_access(_token(user_id="user1"), "user2", _context())

    def test_user_token_without_user_id_rejected(self):
        """Tenant-wide access denied for user-bound token."""
        with pytest.raises(SecurityError, match="Tenant-wide access denied"):
            validate_user_access(_token(user_id="user1"), None, _context())

    def test_no_user_id_on_token_allows_platform(self):
        """Token without user_id allows 'platform' and 'anonym'."""
        validate_user_access(_token(user_id=None), "platform", _context())
        validate_user_access(_token(user_id=None), "anonym", _context())

    def test_no_user_id_on_token_rejects_specific_user(self):
        """Token without user_id rejects specific user access."""
        with pytest.raises(SecurityError, match="User-scoped access"):
            validate_user_access(_token(user_id=None), "specific_user", _context())
