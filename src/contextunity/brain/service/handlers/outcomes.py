"""Authenticated delayed OutcomeObservation ingestion and Brain-owned resolution."""

from __future__ import annotations

import grpc
from contextunity.core import contextunit_pb2
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.permissions import Permissions

from ...core.config import get_core_config
from ...core.exceptions import BrainValidationError
from ...payloads.outcomes import ReportOutcomeObservationPayload
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_token_for_write,
)


class OutcomeObservationHandlersMixin(BrainHandlerBase):
    @grpc_error_handler
    async def ReportOutcomeObservation(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.ADMIN_WRITE)
        config = get_core_config()
        if not config.synapses.outcome_resolver_enabled:
            raise BrainValidationError("Outcome observation resolver is disabled")
        tenant_id = resolve_tenant_id(token)
        params = ReportOutcomeObservationPayload.model_validate(unit.payload or {})
        receipt = await self.storage.resolve_outcome_observation(
            tenant_id=tenant_id,
            observation=params.observation,
            policy_version=config.synapses.outcome_policy_version,
        )
        return make_response(payload=receipt, parent_unit=unit)


__all__ = ["OutcomeObservationHandlersMixin"]
