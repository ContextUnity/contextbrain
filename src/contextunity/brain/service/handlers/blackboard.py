"""Blackboard handlers — Flat Memory Phase A.

Provides gRPC handlers for WriteBlackboard and ReadBlackboard RPCs.
Blackboard is ephemeral scratch data for graph-level pass-by-reference.
"""

from __future__ import annotations

import re

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.permissions import Permissions

from ...payloads import ReadBlackboardPayload, WriteBlackboardPayload
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_contextunit_logger(__name__)

# Validate scope_path: must be valid ltree format (labels separated by dots)
_LTREE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*$")


class BlackboardHandlersMixin(BrainHandlerBase):
    """Mixin for blackboard read/write handlers."""

    @grpc_error_handler
    async def WriteBlackboard(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Write data to the blackboard, return UUID reference.

        Enables pass-by-reference pattern: agents write data to Brain,
        pass UUID through ContextUnit payload, other agents read by UUID.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = WriteBlackboardPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)

        # Validate scope_path format (ltree)
        if not _LTREE_PATTERN.match(params.scope_path):
            from contextunity.brain.core.exceptions import BrainValidationError

            raise BrainValidationError(
                message=(
                    f"Invalid scope_path '{params.scope_path}'. "
                    "Must be valid ltree format: labels separated by dots "
                    "(e.g. 'tenant.project.session.step')."
                ),
            )

        result = await self.storage.write_blackboard(
            tenant_id=params.tenant_id,
            scope_path=params.scope_path,
            content=params.content,
            metadata=params.metadata,
            ttl_seconds=params.ttl_seconds,
            created_by=params.created_by,
        )

        return make_response(
            payload=result,
            parent_unit=unit,
        )

    @grpc_error_handler
    async def ReadBlackboard(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Read blackboard records by UUID(s) — strictly batched.

        Used by graph nodes to resolve pass-by-reference UUIDs to actual data.
        Records past their TTL are automatically excluded.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = ReadBlackboardPayload.model_validate(unit.payload or {})

        # Resolve tenant from token
        tenant_id = token.allowed_tenants[0] if token and token.allowed_tenants else "default"

        records = await self.storage.read_blackboard(
            ids=params.ids,
            tenant_id=tenant_id,
        )

        return make_response(
            payload={"records": records},
            parent_unit=unit,
        )


__all__ = ["BlackboardHandlersMixin"]
