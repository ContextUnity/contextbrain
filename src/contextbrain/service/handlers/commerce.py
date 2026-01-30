"""Commerce handlers - products, enrichment, verifications."""

from __future__ import annotations

import json

from contextcore import get_context_unit_logger

from ...core.exceptions import grpc_error_handler, grpc_stream_error_handler
from ...payloads import GetPendingPayload, SubmitVerificationPayload
from ..helpers import make_response, parse_unit

logger = get_context_unit_logger(__name__)


class CommerceHandlersMixin:
    """Mixin for Commerce/Gardener operations on BrainService."""

    @grpc_stream_error_handler
    async def GetPendingVerifications(self, request, context):
        """Stream items for manual review (Gardener)."""
        unit = parse_unit(request)
        GetPendingPayload(**unit.payload)

        # TODO: Implement pending items queue
        yield make_response(
            payload={"id": "", "content": "", "context_json": "{}"},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:get_pending"],
        )

    @grpc_error_handler
    async def SubmitVerification(self, request, context):
        """Write enrichment results from Gardener."""
        unit = parse_unit(request)
        params = SubmitVerificationPayload(**unit.payload)

        try:
            enrichment = json.loads(params.enrichment_json)
        except json.JSONDecodeError:
            enrichment = {}

        logger.info(f"Received verification for {params.id}: {enrichment}")
        return make_response(
            payload={"success": True},
            trace_id=str(unit.trace_id),
            provenance=list(unit.provenance) + ["brain:submit_verification"],
        )


__all__ = ["CommerceHandlersMixin"]
