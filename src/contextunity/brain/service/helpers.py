"""Helper functions for gRPC service.
IMPORTANT: validate_token_* functions are synchronous. They cannot
``await context.abort()`` (async gRPC).  Instead they raise exceptions
that are caught by ``@grpc_error_handler`` which does the proper
``await context.abort()``.
"""

from __future__ import annotations

from typing import Literal

import grpc
from contextunity.core import ContextToken, ContextUnit
from contextunity.core.exceptions import ContextUnityError, SecurityError
from contextunity.core.permissions import Permissions
from contextunity.core.pii import contains_pii
from contextunity.core.sdk.service_helpers import make_response, parse_unit
from contextunity.core.tenant_policy import (
    SYSTEM_ALLOWED_CELL_KINDS,
    SYSTEM_ALLOWED_RECORD_KINDS,
    SYSTEM_ALLOWED_SOURCE_TYPES,
    classify_tenant,
    validate_tenant_id,
)


# Fetch from verified auth context instead of re-parsing metadata
def extract_token_from_context(_context: object | None = None) -> ContextToken | None:
    """Extract token from context.

    Args:
        context (object | None): The request context payload.

    Returns:
        ContextToken | None: Token from verified auth context, if any.
    """
    from contextunity.core.authz.context import get_auth_context

    auth_ctx = get_auth_context()
    return auth_ctx.token if auth_ctx else None


def validate_token_for_read(
    _unit: ContextUnit,
    token: ContextToken | None,
    _context: grpc.ServicerContext,
    *,
    required_permission: str | None = None,
) -> None:
    """Validate ContextToken for read operations.

    Prefers ``VerifiedAuthContext`` from interceptor when available.
    Falls back to provided token for backward compatibility.

    Args:
        unit: The ContextUnit being read.
        token: The ContextToken to validate (legacy path).
        context: gRPC servicer context.
        required_permission: Specific permission to check (e.g. ``memory:read``).
            Defaults to ``brain:read``.

    Raises:
        ContextUnityError: If token is invalid or missing required permissions.
    """
    from contextunity.core.authz import authorize, get_auth_context

    # Prefer verified auth context from interceptor
    auth_ctx = get_auth_context()
    if auth_ctx is not None:
        token = auth_ctx.token

    if token is None:
        raise ContextUnityError(code="UNAUTHENTICATED", message="Missing ContextToken")

    if token.is_expired():
        raise ContextUnityError(code="UNAUTHENTICATED", message="ContextToken expired")

    # Use canonical authorize() engine
    decision = authorize(
        auth_ctx if auth_ctx is not None else token,
        permission=required_permission or "brain:read",
        service="brain",
    )
    if decision.denied:
        raise SecurityError(message=decision.reason)


def validate_token_for_write(
    _unit: ContextUnit,
    token: ContextToken | None,
    _context: grpc.ServicerContext,
    *,
    required_permission: str | None = None,
) -> None:
    """Validate ContextToken for write operations.

    Prefers ``VerifiedAuthContext`` from interceptor when available.
    Falls back to provided token for backward compatibility.

    Args:
        unit: The ContextUnit being written.
        token: The ContextToken to validate (legacy path).
        context: gRPC servicer context.
        required_permission: Specific permission to check (e.g. ``memory:write``).
            Defaults to ``brain:write``.

    Raises:
        ContextUnityError: If token is invalid or missing required permissions.
    """
    from contextunity.core.authz import authorize, get_auth_context

    # Prefer verified auth context from interceptor
    auth_ctx = get_auth_context()
    if auth_ctx is not None:
        token = auth_ctx.token

    if token is None:
        raise ContextUnityError(code="UNAUTHENTICATED", message="Missing ContextToken")

    if token.is_expired():
        raise ContextUnityError(code="UNAUTHENTICATED", message="ContextToken expired")

    # Use canonical authorize() engine
    decision = authorize(
        auth_ctx if auth_ctx is not None else token,
        permission=required_permission or "brain:write",
        service="brain",
    )
    if decision.denied:
        raise SecurityError(message=decision.reason)


def validate_tenant_access(
    token: ContextToken | None,
    tenant_id: str,
    _context: grpc.ServicerContext,
    *,
    operation: Literal["read", "write"] = "read",
    record_kind: str | None = None,
) -> None:
    """Validate the token grants access to the specified tenant.

    Args:
        token: The ContextToken to validate (SPOT for tenant identity).
        tenant_id: Legacy tenant identifier from the request payload. MUST
            be empty OR match the token's ``allowed_tenants`` — new
            callers SHOULD omit it and let the token decide.
        context: gRPC servicer context.

    Raises:
        ContextUnityError: If tenant access is denied or payload tenant_id
            contradicts the token.
    """

    if not tenant_id:
        return  # Token-only legacy paths derive the tenant before storage access.
    validate_tenant_id(tenant_id, allow_reserved=True)
    if token is None:
        return  # Missing token is handled by validate_token_for_*.

    if hasattr(token, "can_access_tenant") and not token.can_access_tenant(tenant_id):
        raise SecurityError(
            message=f"Tenant access denied: {tenant_id}",
        )

    tenant_class = classify_tenant(tenant_id)
    has_permission = getattr(token, "has_permission", None)
    if tenant_class == "documentation":
        required = Permissions.DOCS_WRITE if operation == "write" else Permissions.DOCS_READ
        if not callable(has_permission) or not has_permission(required):
            raise SecurityError(
                message=f"Documentation tenant access requires {required}",
            )
    elif tenant_class == "system":
        is_system = getattr(token, "user_namespace", "") == "system"
        is_admin = callable(has_permission) and has_permission(Permissions.ADMIN_ALL)
        if not (is_system or is_admin):
            raise SecurityError(message="System tenant access requires a system or admin token")
        if operation == "write" and record_kind not in SYSTEM_ALLOWED_RECORD_KINDS:
            raise SecurityError(
                message="System tenant accepts only platform-state record kinds",
            )


def validate_tenant_write_policy(
    token: ContextToken | None,
    tenant_id: str,
    _context: grpc.ServicerContext,
    *,
    content: str | None = None,
    cell_kind: str | None = None,
    source_type: str | None = None,
    record_kind: str = "cell",
) -> None:
    """Enforce reserved-tenant write rules after permission validation."""
    validate_tenant_access(
        token,
        tenant_id,
        _context,
        operation="write",
        record_kind=record_kind,
    )
    tenant_class = classify_tenant(tenant_id)
    if tenant_class == "documentation":
        if cell_kind not in {"document", "documentation"} or source_type != "documentation":
            raise SecurityError(
                message="_doc accepts documentation BrainCells only",
            )
    elif tenant_class == "test" and content is not None:
        if contains_pii(content):
            raise SecurityError(message="_test accepts depersonalized fixture content only")
    elif (
        tenant_class == "system"
        and record_kind == "cell"
        and (
            cell_kind not in SYSTEM_ALLOWED_CELL_KINDS
            or source_type not in SYSTEM_ALLOWED_SOURCE_TYPES
        )
    ):
        raise SecurityError(
            message="_system accepts only config/summary platform-state cells",
        )


def resolve_tenant_id(
    token: ContextToken | None,
    payload_tenant_id: str | None = None,
) -> str:
    """Derive the canonical tenant_id for this RPC.

    Fail-closed per contract-boundaries:
    - If explicit payload_tenant_id is given, use it (must be allowed by token if token has scope).
    - Else if token has exactly one allowed_tenant, use it.
    - 0 tenants or >1 tenants without explicit payload_tenant_id -> raise SecurityError.
    Never silently pick allowed_tenants[0] (order-dependent, multi-tenant unsafe).

    Token is SPOT for tenant identity. Matches router resolve_tenant_from_state pattern.

    Args:
        token (ContextToken | None): The security token for authentication.
        payload_tenant_id (str | None): The payload tenant id parameter (explicit).

    Returns:
        str: The resolved tenant_id.

    Raises:
        SecurityError: If cannot unambiguously resolve (0 or >1 without explicit).
    """
    allowed: tuple[str, ...] = ()
    if token is not None:
        allowed = tuple(getattr(token, "allowed_tenants", None) or ())

    if payload_tenant_id:
        if allowed and payload_tenant_id not in allowed:
            raise SecurityError(
                message=(
                    f"Payload tenant_id {payload_tenant_id!r} not in "
                    f"token.allowed_tenants={allowed}"
                ),
            )
        return payload_tenant_id

    if len(allowed) == 1:
        return allowed[0]

    if len(allowed) == 0:
        raise SecurityError(
            message=(
                "Cannot resolve tenant: token has no allowed_tenants "
                "and no explicit tenant_id in payload."
            ),
        )

    # len(allowed) > 1 and no explicit payload_tenant_id
    raise SecurityError(
        message=(
            f"Cannot resolve tenant: multiple allowed_tenants={sorted(allowed)} "
            "and no explicit tenant_id. Provide tenant_id or narrow scope."
        ),
    )


def validate_user_access(
    token: ContextToken | None,
    user_id: str | None,
    _context: grpc.ServicerContext,
) -> None:
    """Validate the token grants access to the specified user_id.

    Args:
        token: The ContextToken to validate.
        user_id: Target user_id from the request payload.
        context: gRPC servicer context.

    Raises:
        grpc.RpcError: If cross-user access is attempted.
    """
    # Security is always enforced.

    if token is None:
        return  # No token → handled by validate_token_for_*

    if getattr(token, "user_id", None) is None:
        if user_id and user_id not in ("platform", "anonym"):
            raise SecurityError(
                message="User-scoped access requires a user-bound token.",
            )
        return

    if user_id is None:
        raise SecurityError(
            message=(
                "Tenant-wide access denied for user-bound token. Must specify matching user_id."
            ),
        )
    if token.user_id != user_id:
        raise SecurityError(
            message=f"Cross-user access denied. Token user: {token.user_id}, Requested: {user_id}",
        )


__all__ = [
    "parse_unit",
    "make_response",
    "extract_token_from_context",
    "validate_token_for_read",
    "validate_token_for_write",
    "validate_tenant_access",
    "validate_tenant_write_policy",
    "resolve_tenant_id",
    "validate_user_access",
]
