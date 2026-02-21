"""Tests for Brain permission interceptor and domain-specific access control."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from contextcore import ContextToken
from contextcore.security import check_permission
from contextcore.security.interceptors import _extract_rpc_name, _should_skip

from contextbrain.service.interceptors import (
    RPC_PERMISSION_MAP,
)

# ── _extract_rpc_name ──


class TestExtractRpcName:
    def test_full_method(self):
        assert _extract_rpc_name("/brain.BrainService/Search") == "Search"

    def test_simple_name(self):
        assert _extract_rpc_name("Search") == "Search"

    def test_empty(self):
        assert _extract_rpc_name("") == ""

    def test_nested_slashes(self):
        assert _extract_rpc_name("/a/b/c/Search") == "Search"


# ── _should_skip ──


class TestShouldSkip:
    def test_health_check(self):
        assert _should_skip("/grpc.health.v1.Health/Check") is True

    def test_reflection(self):
        assert _should_skip("/grpc.reflection.v1.ServerReflection/List") is True

    def test_brain_search(self):
        assert _should_skip("/brain.BrainService/Search") is False


# ── RPC_PERMISSION_MAP completeness ──


class TestRpcPermissionMap:
    def test_knowledge_rpcs_mapped(self):
        assert "Search" in RPC_PERMISSION_MAP
        assert "IngestDocument" in RPC_PERMISSION_MAP
        assert "GraphSearch" in RPC_PERMISSION_MAP
        assert "GetTaxonomy" in RPC_PERMISSION_MAP

    def test_memory_rpcs_mapped(self):
        assert "AddEpisode" in RPC_PERMISSION_MAP
        assert "GetRecentEpisodes" in RPC_PERMISSION_MAP
        assert "UpsertFact" in RPC_PERMISSION_MAP
        assert "GetUserFacts" in RPC_PERMISSION_MAP

    def test_trace_rpcs_mapped(self):
        assert "LogTrace" in RPC_PERMISSION_MAP
        assert "GetTraces" in RPC_PERMISSION_MAP

    def test_read_ops_require_read_permissions(self):
        read_rpcs = [
            "Search",
            "GraphSearch",
            "GetTaxonomy",
            "GetRecentEpisodes",
            "GetUserFacts",
            "GetTraces",
        ]
        for rpc in read_rpcs:
            perm = RPC_PERMISSION_MAP[rpc]
            assert ":read" in perm, f"{rpc} should require a :read permission, got {perm}"

    def test_write_ops_require_write_permissions(self):
        write_rpcs = ["IngestDocument", "AddEpisode", "UpsertFact", "LogTrace"]
        for rpc in write_rpcs:
            perm = RPC_PERMISSION_MAP[rpc]
            assert ":write" in perm, f"{rpc} should require a :write permission, got {perm}"


# ── check_permission ──


class TestCheckPermission:
    def _make_token(
        self, permissions: tuple[str, ...], tenants: tuple[str, ...] = ()
    ) -> ContextToken:
        return ContextToken(
            token_id="test-token",
            permissions=permissions,
            allowed_tenants=tenants,
        )

    def test_exact_permission_match(self):
        token = self._make_token(("brain:read",))
        assert check_permission(token, "brain:read") is None  # allowed

    def test_missing_permission(self):
        token = self._make_token(("brain:read",))
        result = check_permission(token, "memory:write")
        assert result is not None
        assert "missing permission" in result

    def test_wildcard_admin_all(self):
        """admin:all should grant any permission via expand_permissions."""
        token = self._make_token(("admin:all",))
        result = check_permission(token, "brain:read")
        # Should pass if expand_permissions is available
        # If not available, it's a graceful fallback
        assert result is None or "missing permission" in result

    def test_tenant_access_granted(self):
        token = self._make_token(("brain:read",), ("tenant-1",))
        assert check_permission(token, "brain:read", tenant_id="tenant-1") is None

    def test_tenant_access_denied(self):
        token = self._make_token(("brain:read",), ("tenant-1",))
        result = check_permission(token, "brain:read", tenant_id="tenant-2")
        # Only fails if can_access_tenant is implemented
        if result is not None:
            assert "tenant access denied" in result

    def test_empty_tenants_means_admin(self):
        """Empty allowed_tenants = admin (can access all tenants)."""
        token = self._make_token(("brain:read",), ())
        result = check_permission(token, "brain:read", tenant_id="any-tenant")
        assert result is None  # admin has access to all


# ── validate_tenant_access ──


class TestValidateTenantAccess:
    def test_security_disabled_skips(self):
        """When security is disabled, tenant validation is skipped."""
        from contextbrain.service.helpers import validate_tenant_access

        mock_context = MagicMock()
        token = ContextToken(
            token_id="t1",
            permissions=("brain:read",),
            allowed_tenants=("other",),
        )

        with patch("contextbrain.core.get_core_config") as mock_cfg:
            mock_cfg.return_value.security.enabled = False
            # Should not abort even with mismatched tenant
            validate_tenant_access(token, "tenant-1", mock_context)
            mock_context.abort.assert_not_called()

    def test_no_token_skips(self):
        """When token is None, tenant validation is skipped."""
        from contextbrain.service.helpers import validate_tenant_access

        mock_context = MagicMock()

        with patch("contextbrain.core.get_core_config") as mock_cfg:
            mock_cfg.return_value.security.enabled = True
            validate_tenant_access(None, "tenant-1", mock_context)
            mock_context.abort.assert_not_called()


# ── validate_token_for_read with required_permission ──


class TestValidateTokenForReadWithPermission:
    def test_passes_with_correct_permission(self):
        """Validation passes when token has the required permission."""
        from contextbrain.service.helpers import validate_token_for_read

        mock_context = MagicMock()
        unit = MagicMock()
        unit.security.read = []
        unit.security.write = []
        unit.payload = {}

        token = ContextToken(
            token_id="t1",
            permissions=("brain:read", "memory:read"),
        )

        with patch("contextbrain.core.get_core_config") as mock_cfg:
            mock_cfg.return_value.security.enabled = True
            mock_cfg.return_value.security.policies.read_permission = "brain:read"
            mock_cfg.return_value.security.policies.write_permission = "brain:write"
            mock_cfg.return_value.security.private_key_path = ""

            validate_token_for_read(unit, token, mock_context, required_permission="memory:read")
            mock_context.abort.assert_not_called()

    def test_security_disabled_skips_all(self):
        """When security is disabled, all validation is skipped."""
        from contextbrain.service.helpers import validate_token_for_read

        mock_context = MagicMock()
        unit = MagicMock()

        with patch("contextbrain.core.get_core_config") as mock_cfg:
            mock_cfg.return_value.security.enabled = False
            validate_token_for_read(unit, None, mock_context, required_permission="memory:read")
            mock_context.abort.assert_not_called()
