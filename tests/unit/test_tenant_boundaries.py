"""Behavioral checks for reserved tenant access and content rules."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from contextunity.core import ContextToken
from contextunity.core.exceptions import SecurityError
from contextunity.core.permissions import Permissions

from contextunity.brain.service.helpers import (
    validate_tenant_access,
    validate_tenant_write_policy,
)


def _context() -> MagicMock:
    return MagicMock()


def _token(
    *, permissions: tuple[str, ...], tenants: tuple[str, ...], namespace: str = "default"
) -> ContextToken:
    return ContextToken(
        token_id="boundary-check",
        permissions=permissions,
        allowed_tenants=tenants,
        user_namespace=namespace,
    )


def test_documentation_read_requires_documentation_scope() -> None:
    token = _token(permissions=(Permissions.BRAIN_READ,), tenants=("_doc",))
    with pytest.raises(SecurityError, match="docs:read"):
        validate_tenant_access(token, "_doc", _context())


def test_documentation_write_accepts_only_documentation_cells() -> None:
    token = _token(permissions=(Permissions.DOCS_WRITE,), tenants=("_doc",))
    validate_tenant_write_policy(
        token,
        "_doc",
        _context(),
        content="reference",
        cell_kind="documentation",
        source_type="documentation",
    )
    with pytest.raises(SecurityError, match="documentation BrainCells"):
        validate_tenant_write_policy(
            token,
            "_doc",
            _context(),
            content="reference",
            cell_kind="fact",
            source_type="manual",
        )


def test_test_scope_rejects_obvious_personal_data() -> None:
    token = _token(permissions=(Permissions.MEMORY_WRITE,), tenants=("_test",))
    with pytest.raises(SecurityError, match="depersonalized"):
        validate_tenant_write_policy(
            token,
            "_test",
            _context(),
            content="Contact person@example.org",
            source_type="memory",
        )
    validate_tenant_write_policy(
        token,
        "_test",
        _context(),
        content="Synthetic fixture content",
        source_type="memory",
    )


@pytest.mark.parametrize("content", ["IBAN GB82WEST12345698765432", "card 4111 1111 1111 1111"])
def test_test_scope_rejects_financial_identifiers(content: str) -> None:
    token = _token(permissions=(Permissions.MEMORY_WRITE,), tenants=("_test",))
    with pytest.raises(SecurityError, match="depersonalized"):
        validate_tenant_write_policy(
            token,
            "_test",
            _context(),
            content=content,
            source_type="memory",
        )


def test_system_scope_requires_internal_identity() -> None:
    token = _token(permissions=(Permissions.MEMORY_WRITE,), tenants=("_system",))
    with pytest.raises(SecurityError, match="system or admin"):
        validate_tenant_access(token, "_system", _context(), operation="write")
    system_token = _token(
        permissions=(Permissions.MEMORY_WRITE,), tenants=("_system",), namespace="system"
    )
    validate_tenant_access(
        system_token,
        "_system",
        _context(),
        operation="write",
        record_kind="trace",
    )


def test_system_scope_accepts_only_platform_state_cells() -> None:
    token = _token(permissions=(Permissions.BRAIN_WRITE,), tenants=("_system",), namespace="system")
    with pytest.raises(SecurityError, match="platform-state"):
        validate_tenant_write_policy(
            token,
            "_system",
            _context(),
            cell_kind="fact",
            source_type="manual",
        )
    validate_tenant_write_policy(
        token,
        "_system",
        _context(),
        cell_kind="config",
        source_type="config",
    )
