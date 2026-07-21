"""BrainSynapse handlers — Flat Memory Phase B.

Provides gRPC handlers for RecordSynapse, QuerySynapses, and UpdateSynapseQ.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.faults import AGENT_FAULT
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.passbyref import (
    DEFAULT_PASSBYREF_THRESHOLD_BYTES,
    content_hash_of,
    payload_size_bytes,
)
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict, is_json_dict
from pydantic import ValidationError

from ...core.config import get_core_config
from ...core.exceptions import (
    BrainValidationError,
    SynapseFeatureDisabledError,
    SynapseTenantMismatchError,
)
from ...payloads import QuerySynapsesPayload, RecordSynapsePayload, UpdateSynapseQPayload
from ...reward_policy import apply_node_execution_reward, is_trainable_tenant
from ...storage.contracts import BrainStorageProtocol
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_token_for_read,
    validate_token_for_write,
)
from ..read_bulkhead import get_brain_read_bulkhead

logger = get_contextunit_logger(__name__)


def _require_synapses_enabled() -> None:
    if not get_core_config().synapses.enabled:
        raise SynapseFeatureDisabledError()


def _log_validation_fault(exc: ValidationError, *, event_type: str) -> None:
    """Log a DebugBus-ready ``fault_event()`` for a payload validation
    failure (e.g. a missing required field) before it propagates to the
    gRPC error handler's own status-code mapping.

    A raw pydantic ``ValidationError`` alone carries no ``fault_class`` or
    ``event_type`` — this gives a missing/invalid field the same typed,
    classified shape every other Synapse fault gets, without changing the
    gRPC status the caller sees.
    """
    from contextunity.core.faults import fault_event

    event = fault_event(
        exc,
        event_type=event_type,
        error_code="synapse.validation_failed",
        service="ContextBrain",
        component="synapses",
    )
    logger.warning("Synapse payload validation failed: %s", event)


def _has_q_update(params: UpdateSynapseQPayload) -> bool:
    return (
        params.q_action is not None
        or params.q_hypothesis is not None
        or params.q_relevance is not None
        or params.reward_source is not None
    )


def _apply_reward_source(
    params: UpdateSynapseQPayload,
) -> tuple[float | None, float | None, float | None]:
    """Compute absolute Q-values for a ``reward_source``-driven update.

    Returns the payload's own ``q_action``/``q_hypothesis``/``q_relevance``
    unchanged when no ``reward_source`` was requested. A failure
    (``success=False``) whose ``fault_class`` is set and is not
    ``agent_fault`` must not degrade Q automatically — this reports "no
    change" (``None`` for every field) unless an explicit review
    (``review_id``) overrides it, same as the direct-Q-value path.
    """
    if params.reward_source is None:
        return params.q_action, params.q_hypothesis, params.q_relevance

    node_role = params.node_role
    if node_role is None:
        # Unreachable in practice — UpdateSynapseQPayload's own validator
        # already requires node_role whenever reward_source is set.
        raise BrainValidationError(message="reward_source requires node_role")

    success = bool(params.success)
    non_agent_fault = params.fault_class is not None and params.fault_class != AGENT_FAULT
    if not success and non_agent_fault and not params.review_id:
        return None, None, None

    if (
        params.current_q_action is None
        or params.current_q_hypothesis is None
        or params.current_q_relevance is None
    ):
        # Unreachable in practice — UpdateSynapseQPayload validates this for
        # reward_source updates. Keep the guard here so the handler boundary
        # stays fail-closed and statically narrowed.
        raise BrainValidationError(message="reward_source requires current Q baseline")

    current_q = {
        "q_action": params.current_q_action,
        "q_hypothesis": params.current_q_hypothesis,
        "q_relevance": params.current_q_relevance,
    }
    computed = apply_node_execution_reward(
        node_role=node_role, current_q=current_q, success=success
    )
    return (
        computed.get("q_action", params.q_action),
        computed.get("q_hypothesis", params.q_hypothesis),
        computed.get("q_relevance", params.q_relevance),
    )


def _validate_automatic_q_update(tenant_id: str, params: UpdateSynapseQPayload) -> None:
    if not _has_q_update(params) or params.review_id:
        return
    if not is_trainable_tenant(tenant_id):
        raise BrainValidationError(
            message=f"Automated Synapse Q updates are disabled for tenant {tenant_id!r}",
            tenant_id=tenant_id,
        )


async def _maybe_convert_action_data_to_ref(
    storage: BrainStorageProtocol, payload: JsonDict, *, tenant_id: str
) -> JsonDict:
    """Convert an oversized inline ``action_data`` to a PassByRef pointer.

    ``RecordSynapsePayload``'s own validator can only reject an oversized
    inline ``action_data`` (it is synchronous and cannot perform the
    Blackboard write itself) — this runs first, on the raw payload dict,
    so a caller sending a large payload gets automatic conversion instead
    of a rejection telling them to convert it themselves.
    """
    action_data = payload.get("action_data")
    if not is_json_dict(action_data) or payload.get("action_data_ref"):
        return payload
    if payload_size_bytes(action_data) <= DEFAULT_PASSBYREF_THRESHOLD_BYTES:
        return payload

    hash_value = content_hash_of(action_data)
    written = await storage.write_blackboard(
        tenant_id=tenant_id,
        scope_path="synapse.action_data",
        content=action_data,
    )
    converted = dict(payload)
    converted["action_data"] = {}
    converted["action_data_ref"] = str(written["id"])
    converted["content_hash"] = hash_value
    return converted


class SynapseHandlersMixin(BrainHandlerBase):
    """Mixin for BrainSynapse RPC handlers."""

    @grpc_error_handler
    async def RecordSynapse(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Record one BrainSynapse learning trace."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        # Must match BrainPermissionInterceptor's RPC_PERMISSION_MAP entry —
        # Synapse is a memory surface, so the memory:* family is canonical,
        # same as Blackboard/Conversation History.
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        _require_synapses_enabled()
        raw_payload = unit.payload if is_json_dict(unit.payload) else {}
        raw_tenant_id = raw_payload.get("tenant_id")
        payload_tenant = raw_tenant_id if isinstance(raw_tenant_id, str) else None
        # Spoofed payload tenant is a typed policy_fault (SynapseTenantMismatchError),
        # not the generic SECURITY_ERROR from resolve_tenant_id. Check before resolve
        # so the gRPC code maps to BRAIN_SYNAPSE_TENANT_MISMATCH / PERMISSION_DENIED.
        if (
            payload_tenant
            and token is not None
            and hasattr(token, "can_access_tenant")
            and not token.can_access_tenant(payload_tenant)
        ):
            raise SynapseTenantMismatchError(tenant_id=payload_tenant)
        tenant_id = resolve_tenant_id(token, payload_tenant)
        raw_payload = await _maybe_convert_action_data_to_ref(
            self.storage, raw_payload, tenant_id=tenant_id
        )
        # RecordSynapsePayload's own validator also rejects spoofed tenant_id
        # when auth context is set (unit / in-process construction paths).
        try:
            params = RecordSynapsePayload.model_validate(raw_payload)
        except ValidationError as exc:
            _log_validation_fault(exc, event_type="brain.synapse.record.validation_failed")
            raise

        result = await self.storage.record_synapse(
            tenant_id=tenant_id,
            agent_id=params.agent_id,
            action_type=params.action_type,
            action_data=params.action_data,
            action_data_ref=params.action_data_ref,
            thought_trace_ref=params.thought_trace_ref,
            content_hash=params.content_hash,
            graph_name=params.graph_name,
            graph_run_id=params.graph_run_id,
            node_id=params.node_id,
            node_name=params.node_name,
            node_role=params.node_role,
            scope_path=params.scope_path,
            context_summary=params.context_summary,
            client_id=params.client_id,
            fault_class=params.fault_class,
            status=params.status,
            q_action=params.q_action,
            q_hypothesis=params.q_hypothesis,
            q_relevance=params.q_relevance,
            metadata=params.metadata,
        )

        return make_response(
            payload={
                "id": result.get("id"),
                "agent_id": result.get("agent_id"),
                "action_type": result.get("action_type"),
                "node_role": result.get("node_role"),
                "status": result.get("status"),
                "q_action": result.get("q_action"),
                "q_hypothesis": result.get("q_hypothesis"),
                "q_relevance": result.get("q_relevance"),
                "q_composite": result.get("q_composite"),
                "scope_path": result.get("scope_path"),
                "metadata": result.get("metadata", {}),
                "created_at": str(result.get("created_at", "")),
                "updated_at": str(result.get("updated_at", "")),
            },
            parent_unit=unit,
        )

    @grpc_stream_error_handler
    async def QuerySynapses(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Query BrainSynapses, ranked by q_composite and bounded by limit."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.MEMORY_READ)
        _require_synapses_enabled()
        # QuerySynapsesPayload's own validator already rejects a spoofed tenant_id.
        try:
            params = QuerySynapsesPayload.model_validate(unit.payload or {})
        except ValidationError as exc:
            _log_validation_fault(exc, event_type="brain.synapse.query.validation_failed")
            raise
        tenant_id = resolve_tenant_id(token, params.tenant_id)

        async with get_brain_read_bulkhead().acquire(tenant_id):
            rows = await self.storage.query_synapses(
                tenant_id=tenant_id,
                action_type=params.action_type,
                agent_id=params.agent_id,
                node_role=params.node_role,
                status=params.status,
                scope_path=params.scope_path,
                min_q=params.min_q,
                limit=params.limit,
            )

        for row in rows:
            yield make_response(
                payload={
                    "id": row.get("id"),
                    "graph_name": row.get("graph_name"),
                    "graph_run_id": row.get("graph_run_id"),
                    "node_id": row.get("node_id"),
                    "node_name": row.get("node_name"),
                    "agent_id": row.get("agent_id"),
                    "action_type": row.get("action_type"),
                    "action_data": row.get("action_data", {}),
                    "action_data_ref": row.get("action_data_ref"),
                    "context_summary": row.get("context_summary"),
                    "thought_trace_ref": row.get("thought_trace_ref"),
                    "content_hash": row.get("content_hash"),
                    "node_role": row.get("node_role"),
                    "fault_class": row.get("fault_class"),
                    "status": row.get("status"),
                    "q_action": row.get("q_action"),
                    "q_hypothesis": row.get("q_hypothesis"),
                    "q_relevance": row.get("q_relevance"),
                    "q_composite": row.get("q_composite"),
                    "scope_path": row.get("scope_path"),
                    "metadata": row.get("metadata", {}),
                    "created_at": str(row.get("created_at", "")),
                    "updated_at": str(row.get("updated_at", "")),
                },
                parent_unit=unit,
            )

    @grpc_error_handler
    async def UpdateSynapseQ(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Update Q-values/fault/status on one tenant-owned Synapse."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.MEMORY_WRITE)
        _require_synapses_enabled()
        try:
            params = UpdateSynapseQPayload.model_validate(unit.payload or {})
        except ValidationError as exc:
            _log_validation_fault(exc, event_type="brain.synapse.q_update.validation_failed")
            raise
        # UpdateSynapseQPayload carries no tenant_id field by design — the
        # token is the only source of tenant scope, so spoofing isn't possible.
        tenant_id = resolve_tenant_id(token)
        _validate_automatic_q_update(tenant_id, params)
        q_action, q_hypothesis, q_relevance = _apply_reward_source(params)

        result = await self.storage.update_synapse_q(
            tenant_id=tenant_id,
            synapse_id=params.synapse_id,
            q_action=q_action,
            q_hypothesis=q_hypothesis,
            q_relevance=q_relevance,
            fault_class=params.fault_class,
            status=params.status,
            metadata_patch=params.metadata,
            # review_id (explicit review) takes precedence over event_id
            # (automated reward source) when a caller somehow sets both.
            idempotency_key=params.review_id or params.event_id,
        )

        if result is None:
            raise BrainValidationError(
                message=f"Synapse {params.synapse_id!r} not found for this tenant",
            )

        return make_response(
            payload={
                "id": result.get("id"),
                "q_action": result.get("q_action"),
                "q_hypothesis": result.get("q_hypothesis"),
                "q_relevance": result.get("q_relevance"),
                "q_composite": result.get("q_composite"),
                "updated_at": str(result.get("updated_at", "")),
            },
            parent_unit=unit,
        )


__all__ = ["SynapseHandlersMixin"]
