"""Taxonomy handlers - upsert and get taxonomy."""

from __future__ import annotations

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler
from contextcore.permissions import Permissions

from ...payloads import GetTaxonomyPayload, UpsertTaxonomyPayload
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


class TaxonomyHandlersMixin:
    """Mixin for taxonomy operations."""

    @grpc_error_handler
    async def UpsertTaxonomy(self, request, context):
        """Sync YAML-to-DB or UI-to-DB taxonomy entries."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = UpsertTaxonomyPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        await self.storage.upsert_taxonomy(
            tenant_id=params.tenant_id,
            domain=params.domain,
            name=params.name,
            path=params.path,
            keywords=params.keywords,
            metadata=params.metadata,
        )
        return make_response(
            payload={"success": True},
            parent_unit=unit,
            provenance=["brain:upsert_taxonomy"],
        )

    @grpc_stream_error_handler
    async def GetTaxonomy(self, request, context):
        """Export taxonomy from DB."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GetTaxonomyPayload(**unit.payload)
        validate_tenant_access(token, params.tenant_id, context)

        if hasattr(self.storage, "get_all_taxonomy"):
            taxonomies = await self.storage.get_all_taxonomy(
                tenant_id=params.tenant_id, domain=params.domain
            )
            for tax in taxonomies:
                yield make_response(
                    payload={
                        "domain": tax["domain"],
                        "name": tax["name"],
                        "path": tax["path"],
                        "keywords": list(tax["keywords"]),
                        "metadata": dict(tax["metadata"]),
                    },
                    parent_unit=unit,
                    provenance=["brain:get_taxonomy"],
                )


__all__ = ["TaxonomyHandlersMixin"]
