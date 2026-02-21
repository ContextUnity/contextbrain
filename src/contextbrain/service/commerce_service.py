"""CommerceService - extends Brain with product operations."""

from __future__ import annotations

import grpc
from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler

from ..payloads import (
    GetProductsPayload,
    UpdateEnrichmentPayload,
    UpsertDealerProductPayload,
)
from .helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


# Only create if commerce proto available
try:
    from contextcore import commerce_pb2_grpc

    class CommerceService(commerce_pb2_grpc.CommerceServiceServicer):
        """Commerce-specific gRPC service for product operations."""

        def __init__(self, brain_service):
            self._brain = brain_service

        @grpc_stream_error_handler
        async def GetProducts(self, request, context):
            """Get products for enrichment by IDs."""
            unit = parse_unit(request)
            token = extract_token_from_context(context)
            validate_token_for_read(unit, token, context)
            params = GetProductsPayload(**unit.payload)
            validate_tenant_access(token, params.tenant_id, context)

            if hasattr(self._brain.storage, "get_products_by_ids"):
                raw_products = await self._brain.storage.get_products_by_ids(
                    tenant_id=params.tenant_id,
                    product_ids=params.product_ids,
                )
                for p in raw_products:
                    yield make_response(
                        payload={
                            "id": p.get("id", 0),
                            "name": p.get("name", ""),
                            "category": p.get("category", ""),
                            "description": p.get("description", ""),
                            "brand_name": p.get("brand_name", ""),
                            "params": p.get("params", {}),
                            "enrichment": p.get("enrichment", {}),
                        },
                        parent_unit=unit,  # Inherit trace_id and extend provenance
                        provenance=["commerce:get_products"],
                    )

        @grpc_error_handler
        async def UpsertDealerProduct(self, request, context):
            """Upsert dealer product from Harvester."""
            unit = parse_unit(request)
            token = extract_token_from_context(context)
            validate_token_for_write(unit, token, context)
            params = UpsertDealerProductPayload(**unit.payload)
            validate_tenant_access(token, params.tenant_id, context)

            try:
                if hasattr(self._brain.storage, "upsert_dealer_product"):
                    product_id = await self._brain.storage.upsert_dealer_product(
                        tenant_id=params.tenant_id,
                        dealer_code=params.dealer_code,
                        dealer_name=params.dealer_name,
                        sku=params.sku,
                        name=params.name,
                        category=params.category,
                        brand_name=params.brand_name,
                        quantity=params.quantity,
                        price_retail=params.price_retail,
                        currency=params.currency,
                        params=params.params,
                        status=params.status,
                    )
                else:
                    product_id = hash(f"{params.dealer_code}:{params.sku}") % (2**31)

                logger.info(
                    f"Upserted dealer product: {params.dealer_code}/{params.sku} -> {product_id}"
                )
                return make_response(
                    payload={
                        "success": True,
                        "product_id": product_id,
                        "message": "OK",
                    },
                    parent_unit=unit,  # Inherit trace_id and extend provenance
                    provenance=["commerce:upsert_dealer"],
                )
            except Exception as e:
                logger.error(f"UpsertDealerProduct failed: {e}")
                return make_response(
                    payload={"success": False, "product_id": 0, "message": str(e)},
                    parent_unit=unit,  # Inherit trace_id and extend provenance
                    provenance=["commerce:upsert_dealer:error"],
                )

        @grpc_error_handler
        async def UpdateEnrichment(self, request, context):
            """Update product enrichment data."""
            unit = parse_unit(request)
            token = extract_token_from_context(context)
            validate_token_for_write(unit, token, context)
            params = UpdateEnrichmentPayload(**unit.payload)
            validate_tenant_access(token, params.tenant_id, context)

            if hasattr(self._brain.storage, "update_product_enrichment"):
                await self._brain.storage.update_product_enrichment(
                    tenant_id=params.tenant_id,
                    product_id=params.product_id,
                    enrichment=params.enrichment,
                    trace_id=params.trace_id,
                    status=params.status,
                )
                logger.info(f"Updated enrichment for product {params.product_id}")
                return make_response(
                    payload={"success": True},
                    parent_unit=unit,  # Inherit trace_id and extend provenance
                    provenance=["commerce:update_enrichment"],
                )

            return make_response(
                payload={"success": False},
                parent_unit=unit,  # Inherit trace_id and extend provenance
                provenance=["commerce:update_enrichment:not_supported"],
            )

        @grpc_error_handler
        async def GetProduct(self, request, context):
            """Placeholder for single product retrieval."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

        @grpc_error_handler
        async def UpdateProduct(self, request, context):
            """Placeholder for product update."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

        @grpc_error_handler
        async def TriggerHarvest(self, request, context):
            """Placeholder for harvest trigger."""
            return context.abort(grpc.StatusCode.UNIMPLEMENTED, "Not implemented")

        @grpc_stream_error_handler
        async def GetPendingVerifications(self, request, context):
            async for item in self._brain.GetPendingVerifications(request, context):
                yield item

        @grpc_error_handler
        async def SubmitVerification(self, request, context):
            return await self._brain.SubmitVerification(request, context)

    HAS_COMMERCE = True

except ImportError:
    CommerceService = None
    HAS_COMMERCE = False

__all__ = ["CommerceService", "HAS_COMMERCE"]
