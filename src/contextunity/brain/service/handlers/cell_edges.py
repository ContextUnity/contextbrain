"""Retained Phase 5 CellEdge traversal and relation handlers."""

from __future__ import annotations

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.permissions import Permissions

from ...payloads import CreateKGRelationPayload, GraphSearchPayload
from ...storage.postgres.models import GraphEdge
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
    validate_user_access,
)

logger = get_contextunit_logger(__name__)


class CellEdgeHandlersMixin(BrainHandlerBase):
    """Current graph surface retained until the Phase 5 CellEdge replacement."""

    @grpc_error_handler
    async def GraphSearch(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Traverse bounded current CellEdge rows from explicit entrypoints."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GraphSearchPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)
        result = await self.storage.graph_search(
            tenant_id=params.tenant_id,
            user_id=params.user_id,
            entrypoint_ids=params.entrypoint_ids,
            max_hops=params.max_hops,
            allowed_relations=params.allowed_relations or None,
            max_results=params.max_results,
        )
        logger.info(
            "GraphSearch: tenant=%s entrypoints=%d hops=%d -> nodes=%d edges=%d",
            params.tenant_id,
            len(params.entrypoint_ids),
            params.max_hops,
            len(result.get("nodes", [])),
            len(result.get("edges", [])),
        )
        return make_response(
            payload={"nodes": result.get("nodes", []), "edges": result.get("edges", [])},
            parent_unit=unit,
        )

    @grpc_error_handler
    async def CreateKGRelation(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Create one retained CellEdge relation under tenant authority."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = CreateKGRelationPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)
        validate_user_access(token, params.user_id, context)
        edge = GraphEdge(
            source_id=f"{params.source_type}:{params.source_id}",
            target_id=f"{params.target_type}:{params.target_id}",
            relation=params.relation,
            weight=1.0,
            metadata={},
        )
        await self.storage.upsert_graph(
            nodes=[],
            edges=[edge],
            tenant_id=params.tenant_id,
            user_id=params.user_id,
        )
        logger.info(
            "Created CellEdge relation: tenant=%s relation=%s",
            params.tenant_id,
            params.relation,
        )
        return make_response(payload={"success": True}, parent_unit=unit)


__all__ = ["CellEdgeHandlersMixin"]
