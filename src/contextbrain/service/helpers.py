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
from contextcore import (
    ContextToken,
    ContextUnit,
    context_unit_pb2,
    extract_token_from_grpc_metadata,
)
from contextcore.exceptions import ContextUnityError

from contextbrain.core.tokens import AccessManager


def parse_unit(request) -> ContextUnit:
    """Parse protobuf request to ContextUnit."""
    return ContextUnit.from_protobuf(request)


# Re-export from contextcore for backward compatibility
extract_token_from_context = extract_token_from_grpc_metadata


def validate_token_for_read(
    unit: ContextUnit,
    token: Optional[ContextToken],
    context: grpc.ServicerContext,
    *,
    required_permission: str | None = None,
) -> None:
    """Validate ContextToken for read operations.

    Args:
        unit: The ContextUnit being read.
        token: The ContextToken to validate.
        context: gRPC servicer context.
        required_permission: Specific permission to check (e.g. ``memory:read``).
            Falls back to ``config.security.policies.read_permission``.

    Raises:
        grpc.RpcError if token is invalid or missing required permissions
    """
    from contextbrain.core import get_core_config

    config = get_core_config()
    if not config.security.enabled:
        return  # Security disabled, skip validation

    if token is None:
        raise ContextUnityError(code="UNAUTHENTICATED", message="Missing ContextToken")

    if token.is_expired():
        raise ContextUnityError(code="UNAUTHENTICATED", message="ContextToken expired")

    access = AccessManager.from_core_config()
    try:
        if required_permission:
            # Domain-specific: check only the required permission
            access.verify_read(token, permission=required_permission)
        else:
            # Generic: check default brain:read + unit-level scopes
            access.verify_unit_read(unit, token)
    except PermissionError as e:
        raise ContextUnityError(code="PERMISSION_DENIED", message=str(e)) from e


def validate_token_for_write(
    unit: ContextUnit,
    token: Optional[ContextToken],
    context: grpc.ServicerContext,
    *,
    required_permission: str | None = None,
) -> None:
    """Validate ContextToken for write operations.

    Args:
        unit: The ContextUnit being written.
        token: The ContextToken to validate.
        context: gRPC servicer context.
        required_permission: Specific permission to check (e.g. ``memory:write``).
            Falls back to ``config.security.policies.write_permission``.

    Raises:
        grpc.RpcError if token is invalid or missing required permissions
    """
    from contextbrain.core import get_core_config

    config = get_core_config()
    if not config.security.enabled:
        return  # Security disabled, skip validation

    if token is None:
        raise ContextUnityError(code="UNAUTHENTICATED", message="Missing ContextToken")

    if token.is_expired():
        raise ContextUnityError(code="UNAUTHENTICATED", message="ContextToken expired")

    access = AccessManager.from_core_config()
    try:
        if required_permission:
            # Domain-specific: check only the required permission
            access.verify_write(token, permission=required_permission)
        else:
            # Generic: check default brain:write + unit-level scopes
            access.verify_unit_write(unit, token)
    except PermissionError as e:
        raise ContextUnityError(code="PERMISSION_DENIED", message=str(e)) from e


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
    from contextbrain.core import get_core_config

    config = get_core_config()
    if not config.security.enabled:
        return

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
    from contextbrain.core import get_core_config

    config = get_core_config()
    if not config.security.enabled:
        return

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
    provenance: list[str] | None = None,
    parent_unit: ContextUnit | None = None,
) -> bytes:
    """Create ContextUnit response protobuf.

    Args:
        payload: Response payload data
        trace_id: Trace identifier (inherited from parent_unit if None)
        provenance: Provenance chain (extended from parent_unit if None)
        parent_unit: Optional parent ContextUnit to inherit trace_id/provenance from

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

    # Extend provenance from parent if provided
    if provenance is None:
        if parent_unit:
            provenance = list(parent_unit.provenance) + ["brain:response"]
        else:
            provenance = ["brain:response"]
    elif parent_unit:
        # Extend parent provenance if both provided
        provenance = list(parent_unit.provenance) + provenance

    unit = ContextUnit(
        payload=payload,
        trace_id=trace_id,
        provenance=provenance,
    )
    return unit.to_protobuf(context_unit_pb2)


__all__ = ["parse_unit", "make_response"]
