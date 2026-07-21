"""UniversalDebugBus ContextUnit handlers over the Brain-local application port."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID

import grpc
from contextunity.core import contextunit_pb2
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.permissions import Permissions

from contextunity.brain.core.config import get_core_config
from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.payloads.udb import (
    GetDebugCasePayload,
    QueryDebugCasesPayload,
    ReopenDebugCasePayload,
    ReportFaultOccurrencePayload,
    ReportMitigationAttemptPayload,
    ReportRecoveryEvidencePayload,
    ResolveDebugCasePayload,
)
from contextunity.brain.udb_application import UdbApplication

from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_token_for_read,
    validate_token_for_write,
)


class UdbHandlersMixin(BrainHandlerBase):
    """Complete tenant-scoped UDB lifecycle and bounded query handlers."""

    def _udb_application(self) -> UdbApplication:
        return UdbApplication(storage=self.storage, enabled=get_core_config().udb.enabled)

    @grpc_error_handler
    async def ReportFaultOccurrence(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ReportFaultOccurrencePayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.occurrence.tenant_id)
        if tenant_id != params.occurrence.tenant_id:
            raise BrainValidationError("fault occurrence tenant does not match token scope")
        case = await self._udb_application().report_fault_occurrence(params.occurrence)
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    @grpc_error_handler
    async def ReportRecoveryEvidence(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ReportRecoveryEvidencePayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.evidence.comparison_key.tenant_id)
        case = await self._udb_application().report_recovery_evidence(
            tenant_id=tenant_id, evidence=params.evidence
        )
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    @grpc_error_handler
    async def ReportMitigationAttempt(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ReportMitigationAttemptPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token)
        case = await self._udb_application().report_mitigation_attempt(
            tenant_id=tenant_id, attempt=params.attempt
        )
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    @grpc_error_handler
    async def ResolveDebugCase(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ResolveDebugCasePayload.model_validate(unit.payload or {})
        case = await self._udb_application().resolve_debug_case(
            tenant_id=resolve_tenant_id(token), command=params.command
        )
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    @grpc_error_handler
    async def ReopenDebugCase(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.TRACE_WRITE)
        params = ReopenDebugCasePayload.model_validate(unit.payload or {})
        case = await self._udb_application().reopen_debug_case(
            tenant_id=resolve_tenant_id(token), command=params.command
        )
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    @grpc_error_handler
    async def GetDebugCase(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.TRACE_READ)
        params = GetDebugCasePayload.model_validate(unit.payload or {})
        try:
            case_id = UUID(params.case_id)
        except ValueError as exc:
            raise BrainValidationError("case_id must be a UUID") from exc
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        if params.include_history:
            if params.history_limit > get_core_config().udb.max_query_limit:
                raise BrainValidationError("UDB history limit exceeds operator maximum")
            detail = await self._udb_application().get_debug_case_detail(
                tenant_id=tenant_id,
                case_id=case_id,
                history_limit=params.history_limit,
            )
            if detail is None:
                raise BrainValidationError("DebugCase not found for tenant")
            return make_response(
                payload={"detail": detail.model_dump(mode="json")},
                parent_unit=unit,
            )
        case = await self._udb_application().get_debug_case(
            tenant_id=tenant_id,
            case_id=case_id,
        )
        if case is None:
            raise BrainValidationError("DebugCase not found for tenant")
        return make_response(payload={"case": case.model_dump(mode="json")}, parent_unit=unit)

    async def _query(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
        *,
        recurring_only: bool,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.TRACE_READ)
        params = QueryDebugCasesPayload.model_validate(unit.payload or {})
        if params.query.limit > get_core_config().udb.max_query_limit:
            raise BrainValidationError("UDB query limit exceeds operator maximum")
        cases = await self._udb_application().query_debug_cases(
            tenant_id=resolve_tenant_id(token, params.tenant_id),
            query=params.query,
            recurring_only=recurring_only,
        )
        for case in cases:
            yield make_response(
                payload={"case": case.model_dump(mode="json")},
                parent_unit=unit,
            )

    @grpc_stream_error_handler
    async def QueryDebugCases(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        async for response in self._query(request, context, recurring_only=False):
            yield response

    @grpc_stream_error_handler
    async def QueryRecurringFaults(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        async for response in self._query(request, context, recurring_only=True):
            yield response


__all__ = ["UdbHandlersMixin"]
