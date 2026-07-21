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

    def test_payload_tenant_not_in_token_raises(self):
        """Explicit tenant must be in allowed; mismatch is fail-closed (no silent pick)."""
        token = _token(tenants=["acme"])
        with pytest.raises(SecurityError):
            resolve_tenant_id(token, "other")

    def test_no_token_rejects_payload_selector(self):
        with pytest.raises(SecurityError, match="verified ContextToken"):
            resolve_tenant_id(None, "legacy")

    def test_no_token_no_payload_fails_closed(self):
        from contextunity.core.exceptions import SecurityError

        with pytest.raises(SecurityError):
            resolve_tenant_id(None)

    def test_token_without_tenants_rejects_payload(self):
        token = _token(tenants=None)
        with pytest.raises(SecurityError, match="outside token scope"):
            resolve_tenant_id(token, "fallback")

    def test_token_with_empty_tenants_rejects_payload(self):
        token = _token(tenants=[])
        with pytest.raises(SecurityError, match="outside token scope"):
            resolve_tenant_id(token, "legacy")

    def test_multi_tenant_without_explicit_payload_fails_closed(self):
        """>1 allowed_tenants requires explicit tenant_id; no [0] pick (order-dependent unsafe)."""
        token = _token(tenants=["acme", "corp"])
        with pytest.raises(SecurityError, match="multiple allowed_tenants"):
            resolve_tenant_id(token)


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

    def test_doc_write_without_authority_classifies_as_policy_fault(self):
        """`_doc` gets no special-case bypass — a token without `_doc` in
        allowed_tenants is rejected the same way as any other tenant, and
        that rejection classifies as policy_fault (not agent_fault), so
        it never degrades a Synapse Q-value if fed through the fault
        pipeline. The real end-to-end RPC-level rejection is proven in
        tests/integration_inproc/test_docs_as_memory_authorization_inproc.py;
        this ties that same SecurityError to its fault classification."""
        from contextunity.core.faults import classify_exception

        with pytest.raises(SecurityError, match="Tenant access denied") as exc_info:
            validate_tenant_access(_token(tenants=["project-a"]), "_doc", _context())

        assert classify_exception(exc_info.value) == "policy_fault"


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
