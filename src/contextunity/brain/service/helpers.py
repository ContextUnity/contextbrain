"""Helper functions for gRPC service.

IMPORTANT: validate_token_* functions are synchronous. They cannot
``await context.abort()`` (async gRPC).  Instead they raise exceptions
that are caught by ``@grpc_error_handler`` which does the proper
``await context.abort()``.
"""

from __future__ import annotations

import uuid
from typing import Optional
from uuid import UUID

import grpc
from contextunity.core import (
    ContextToken,
    ContextUnit,
    contextunit_pb2,
)
from contextunity.core.exceptions import ContextUnityError


def parse_unit(request) -> ContextUnit:
    """Parse protobuf request to ContextUnit."""
    return ContextUnit.from_protobuf(request)


# Fetch from verified auth context instead of re-parsing metadata
def extract_token_from_context(context=None) -> Optional[ContextToken]:
    from contextunity.core.authz.context import get_auth_context

    auth_ctx = get_auth_context()
    return auth_ctx.token if auth_ctx else None


def validate_token_for_read(
    unit: ContextUnit,
    token: Optional[ContextToken],
    context: grpc.ServicerContext,
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
        ContextUnityError if token is invalid or missing required permissions
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
        raise ContextUnityError(code="PERMISSION_DENIED", message=decision.reason)


def validate_token_for_write(
    unit: ContextUnit,
    token: Optional[ContextToken],
    context: grpc.ServicerContext,
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
        ContextUnityError if token is invalid or missing required permissions
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
        raise ContextUnityError(code="PERMISSION_DENIED", message=decision.reason)


def validate_tenant_access(
    token: Optional[ContextToken],
    tenant_id: str,
    context: grpc.ServicerContext,
) -> None:
    """Validate the token grants access to the specified tenant.

    Args:
        token: The ContextToken to validate.
        tenant_id: Tenant identifier from the request payload.
        context: gRPC servicer context.

    Raises:
        grpc.RpcError with PERMISSION_DENIED if tenant access denied.
    """
    # Security is always enforced.

    if token is None:
        return  # No token → handled by validate_token_for_*

    if hasattr(token, "can_access_tenant") and not token.can_access_tenant(tenant_id):
        raise ContextUnityError(
            code="PERMISSION_DENIED",
            message=f"Tenant access denied: {tenant_id}",
        )


def validate_user_access(
    token: Optional[ContextToken],
    user_id: str | None,
    context: grpc.ServicerContext,
) -> None:
    """Validate the token grants access to the specified user_id.

    Args:
        token: The ContextToken to validate.
        user_id: Target user_id from the request payload.
        context: gRPC servicer context.

    Raises:
        grpc.RpcError with PERMISSION_DENIED if cross-user access attempted.
    """
    # Security is always enforced.

    if token is None:
        return  # No token → handled by validate_token_for_*

    if hasattr(token, "user_id") and token.user_id is not None:
        if user_id is None:
            raise ContextUnityError(
                code="PERMISSION_DENIED",
                message="Tenant-wide access denied for user-bound token. Must specify matching user_id.",
            )
        if token.user_id != user_id:
            raise ContextUnityError(
                code="PERMISSION_DENIED",
                message=f"Cross-user access denied. Token user: {token.user_id}, Requested: {user_id}",
            )


def make_response(
    payload: dict,
    trace_id: str | UUID | None = None,
    parent_unit: ContextUnit | None = None,
) -> bytes:
    """Create ContextUnit response protobuf.

    Args:
        payload: Response payload data
        trace_id: Trace identifier (inherited from parent_unit if None)
        parent_unit: Optional parent ContextUnit to inherit trace_id from

    Returns:
        Serialized protobuf bytes
    """
    from uuid import UUID

    # Inherit trace_id from parent if not provided
    if trace_id is None and parent_unit:
        trace_id = parent_unit.trace_id
    elif isinstance(trace_id, str):
        trace_id = UUID(trace_id)
    elif trace_id is None:
        trace_id = uuid.uuid4()

    unit = ContextUnit(
        payload=payload,
        trace_id=trace_id,
    )
    return unit.to_protobuf(contextunit_pb2)


__all__ = ["parse_unit", "make_response"]
