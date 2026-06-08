"""Helper functions for gRPC service.
IMPORTANT: validate_token_* functions are synchronous. They cannot
``await context.abort()`` (async gRPC).  Instead they raise exceptions
that are caught by ``@grpc_error_handler`` which does the proper
``await context.abort()``.
"""

from __future__ import annotations

import grpc
from contextunity.core import ContextToken, ContextUnit
from contextunity.core.exceptions import ContextUnityError, SecurityError
from contextunity.core.sdk.service_helpers import make_response, parse_unit


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

    if token is None:
        return  # Missing token is handled by validate_token_for_*.

    if not tenant_id:
        return  # Token-only mode: server derives tenant from the token.

    if hasattr(token, "can_access_tenant") and not token.can_access_tenant(tenant_id):
        raise SecurityError(
            message=f"Tenant access denied: {tenant_id}",
        )


def resolve_tenant_id(
    token: ContextToken | None,
    payload_tenant_id: str | None = None,
) -> str:
    """Derive the canonical tenant_id for this RPC.

    Args:
        token (ContextToken | None): The security token for authentication.
        payload_tenant_id (str | None): The payload tenant id parameter.

    Returns:
        str: The resulting string value.
    """
    if token is not None and getattr(token, "allowed_tenants", None):
        allowed = token.allowed_tenants
        if payload_tenant_id and payload_tenant_id in allowed:
            return payload_tenant_id
        return allowed[0]
    return payload_tenant_id or "default"


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
            message="Tenant-wide access denied for user-bound token. Must specify matching user_id.",
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
    "resolve_tenant_id",
    "validate_user_access",
]
