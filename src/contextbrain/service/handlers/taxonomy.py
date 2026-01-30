"""Taxonomy handlers - upsert and get taxonomy."""

from __future__ import annotations

from contextcore import get_context_unit_logger

from ...core.exceptions import grpc_error_handler, grpc_stream_error_handler
from ...payloads import GetTaxonomyPayload, UpsertTaxonomyPayload
from ..helpers import make_response, parse_unit

logger = get_context_unit_logger(__name__)


class TaxonomyHandlersMixin:
    """Mixin for taxonomy operations."""

    @grpc_error_handler
    async def UpsertTaxonomy(self, request, context):
        """Sync YAML-to-DB or UI-to-DB taxonomy entries."""
        unit = parse_unit(request)
        params = UpsertTaxonomyPayload(**unit.payload)

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
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:upsert_taxonomy"],
        )

    @grpc_stream_error_handler
    async def GetTaxonomy(self, request, context):
        """Export taxonomy from DB."""
        unit = parse_unit(request)
        params = GetTaxonomyPayload(**unit.payload)

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
                    trace_id=str(unit.trace_id),
                    provenance=list(unit.provenance) + ["brain:get_taxonomy"],
                )


__all__ = ["TaxonomyHandlersMixin"]
