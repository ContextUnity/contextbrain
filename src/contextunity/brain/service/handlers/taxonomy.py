"""Taxonomy handlers - upsert and get taxonomy."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import is_json_dict, is_object_list

from ...payloads import GetTaxonomyPayload, UpsertTaxonomyPayload
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


class TaxonomyHandlersMixin(BrainHandlerBase):
    """Mixin for taxonomy operations."""

    @grpc_error_handler
    async def UpsertTaxonomy(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Sync YAML-to-DB or UI-to-DB taxonomy entries."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertTaxonomyPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)

        await self.storage.upsert_taxonomy(
            tenant_id=params.tenant_id,
            domain=params.domain,
            name=params.name,
            path=params.path or "",
            keywords=params.keywords,
            metadata=params.metadata,
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def GetTaxonomy(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Export taxonomy from DB."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GetTaxonomyPayload.model_validate(unit.payload or {})
        validate_tenant_access(token, params.tenant_id, context)

        if hasattr(self.storage, "get_all_taxonomy"):
            taxonomies = await self.storage.get_all_taxonomy(
                tenant_id=params.tenant_id, domain=params.domain
            )
            for tax in taxonomies:
                keywords_raw = tax.get("keywords")
                keywords = (
                    [str(item) for item in keywords_raw] if is_object_list(keywords_raw) else []
                )
                metadata_raw = tax.get("metadata")
                metadata = dict(metadata_raw) if is_json_dict(metadata_raw) else {}
                yield make_response(
                    payload={
                        "domain": str(tax.get("domain", "")),
                        "name": str(tax.get("name", "")),
                        "path": str(tax.get("path", "")),
                        "keywords": keywords,
                        "metadata": metadata,
                    },
                    parent_unit=unit,
                )


__all__ = ["TaxonomyHandlersMixin"]
