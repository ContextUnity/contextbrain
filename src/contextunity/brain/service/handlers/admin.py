"""Admin handler mixin — cross-tenant observability RPCs (WS-8).

Replaces View ``brain_db`` RLS bypass (security finding S8).
All 11 RPCs require ``admin:read``.

Tenant scoping rules (enforced here — NEVER in the proto/interceptor alone):

- Token with ``admin:all``: may omit ``tenant_id`` → queries all tenants.
- Token without ``admin:all``: MUST supply a ``tenant_id`` that is in
  ``token.allowed_tenants``.  Empty ``allowed_tenants`` is NEVER
  interpreted as "all tenants" — the call fails with PERMISSION_DENIED.

All DB access goes through ``self._admin_ops`` (``AdminQueryProtocol``),
which uses Brain's internal admin conn:
``self.storage.tenant_connection('*', user_id='*')`` on Postgres backends.
"""

from __future__ import annotations

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.exceptions import SecurityError
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.tokens import ContextToken

from ...payloads import (
    AdminGetAnalyticsSummaryPayload,
    AdminGetCellsPayload,
    AdminGetFilterOptionsPayload,
    AdminGetMemoryLayerStatsPayload,
    AdminGetRelatedEpisodesPayload,
    AdminGetSessionTracesPayload,
    AdminGetSystemAnalyticsPayload,
    AdminGetTraceDetailsPayload,
    AdminSearchEpisodesPayload,
    AdminSearchTracesPayload,
    ListTenantsPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_token_for_read,
)

logger = get_contextunit_logger(__name__)


def _require_admin_tenant_scope(
    token: ContextToken | object | None, tenant_id: str | None, rpc_name: str
) -> str | None:
    """Enforce tenant scoping for admin RPCs.

    Returns the resolved tenant_id (or None = all tenants, only when admin:all).

    Raises:
        SecurityError: if the scoping contract is violated.
    """
    if not isinstance(token, ContextToken):
        raise SecurityError(message="Missing ContextToken for admin RPC")

    has_admin_all = token.has_permission(Permissions.ADMIN_ALL)
    if has_admin_all:
        return tenant_id if tenant_id else None

    if not tenant_id:
        raise SecurityError(
            message=(
                f"{rpc_name}: tenant_id is required when token does not have admin:all. "
                "Empty allowed_tenants is never treated as cross-tenant access."
            )
        )

    if not token.can_access_tenant(tenant_id):
        raise SecurityError(
            message=(
                f"{rpc_name}: tenant access denied for tenant_id={tenant_id!r}. "
                f"Token allowed_tenants={list(token.allowed_tenants)!r}"
            )
        )

    return tenant_id


def _token_can_view_tenant(token: ContextToken | object | None, tenant_id: str) -> bool:
    """Whether an admin token may view a resource that belongs to ``tenant_id``."""
    if not isinstance(token, ContextToken):
        return False
    if token.has_permission(Permissions.ADMIN_ALL):
        return True
    return bool(tenant_id) and token.can_access_tenant(tenant_id)


class AdminHandlersMixin(BrainHandlerBase):
    """Mixin for Brain Admin cross-tenant observability RPCs (WS-8)."""

    @grpc_error_handler
    async def ListTenants(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """List tenants the caller may administer."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        _ = ListTenantsPayload.model_validate(unit.payload or {})

        if not isinstance(token, ContextToken):
            raise SecurityError(message="Missing ContextToken for ListTenants")

        rows = await self._admin_ops.list_tenants()
        result_tenants = [
            row for row in rows if _token_can_view_tenant(token, str(row.get("id") or ""))
        ]
        return make_response(payload={"tenants": result_tenants}, parent_unit=unit)

    @grpc_error_handler
    async def AdminSearchTraces(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Cross-tenant trace search.

        ``service`` and ``status`` payload filters are not yet supported by storage;
        they are ignored until a backend implements them.
        """
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminSearchTracesPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(token, params.tenant_id, "AdminSearchTraces")

        traces, total = await self._admin_ops.search_traces(
            tenant_id=resolved_tenant,
            agent_id=params.agent_id,
            hours=params.hours,
            limit=params.limit,
            offset=params.offset,
        )
        return make_response(payload={"traces": traces, "total": total}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetTraceDetails(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Get full trace details by trace_id (cross-tenant, admin-only)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetTraceDetailsPayload.model_validate(unit.payload or {})

        trace = await self._admin_ops.get_trace_details(params.trace_id)
        if trace is None:
            return make_response(payload={"trace": None}, parent_unit=unit)
        if not _token_can_view_tenant(token, str(trace.get("tenant_id") or "")):
            return make_response(payload={"trace": None}, parent_unit=unit)
        return make_response(payload={"trace": trace}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetSystemAnalytics(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Cross-tenant system analytics aggregates."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetSystemAnalyticsPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminGetSystemAnalytics"
        )

        analytics = await self._admin_ops.get_system_analytics(
            tenant_id=resolved_tenant,
            hours=params.hours,
        )
        return make_response(payload={"analytics": analytics}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetMemoryLayerStats(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Cross-tenant memory layer stats."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetMemoryLayerStatsPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminGetMemoryLayerStats"
        )

        layer_stats = await self._admin_ops.get_memory_layer_stats(tenant_id=resolved_tenant)
        if params.layer:
            layer_stats = {params.layer: layer_stats.get(params.layer, {})}
        return make_response(payload={"layer_stats": layer_stats}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetFilterOptions(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Distinct filter values from event_journal for admin UI dropdowns."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetFilterOptionsPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminGetFilterOptions"
        )

        filter_options = await self._admin_ops.get_filter_options(tenant_id=resolved_tenant)
        return make_response(payload={"filter_options": filter_options}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetSessionTraces(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Fetch all traces for a given session_id (cross-tenant, admin-only)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetSessionTracesPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminGetSessionTraces"
        )

        traces = await self._admin_ops.get_session_traces(
            session_id=params.session_id,
            tenant_id=resolved_tenant,
        )
        return make_response(payload={"traces": traces}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetRelatedEpisodes(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Fetch episodic_events related to a trace by trace_id (cross-tenant)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetRelatedEpisodesPayload.model_validate(unit.payload or {})

        trace_tenant = await self._admin_ops.get_trace_tenant(params.trace_id)
        if not trace_tenant or not _token_can_view_tenant(token, trace_tenant):
            return make_response(payload={"episodes": []}, parent_unit=unit)

        episodes = await self._admin_ops.get_related_episodes(params.trace_id)
        return make_response(payload={"episodes": episodes}, parent_unit=unit)

    @grpc_error_handler
    async def AdminSearchEpisodes(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Cross-tenant episodic event search with pagination."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminSearchEpisodesPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminSearchEpisodes"
        )

        events, total = await self._admin_ops.search_episodes(
            tenant_id=resolved_tenant,
            user_id=params.user_id,
            session_id=params.session_id,
            hours=params.hours,
            limit=params.limit,
            offset=params.offset,
        )
        return make_response(payload={"events": events, "total": total}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetCells(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """List cells with optional tenant/kind filter (cross-tenant)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetCellsPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(token, params.tenant_id, "AdminGetCells")

        nodes = await self._admin_ops.get_cells(
            tenant_id=resolved_tenant,
            kind=params.kind,
            limit=params.limit,
        )
        return make_response(payload={"nodes": nodes}, parent_unit=unit)

    @grpc_error_handler
    async def AdminGetAnalyticsSummary(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Rich analytics summary with per-hour breakdown, token costs, tool usage."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.ADMIN_READ)
        params = AdminGetAnalyticsSummaryPayload.model_validate(unit.payload or {})
        resolved_tenant = _require_admin_tenant_scope(
            token, params.tenant_id, "AdminGetAnalyticsSummary"
        )

        analytics = await self._admin_ops.get_analytics_summary(
            tenant_id=resolved_tenant,
            hours=params.hours,
        )
        return make_response(payload={"analytics": analytics}, parent_unit=unit)


__all__ = ["AdminHandlersMixin"]
