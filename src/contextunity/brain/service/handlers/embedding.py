"""Brain handlers for durable asynchronous cell embedding enrichment."""

from __future__ import annotations

import grpc
from contextunity.core import contextunit_pb2
from contextunity.core.exceptions import SecurityError
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.permissions import Permissions
from contextunity.core.types import JsonDict

from ...core.config import EmbeddingEnrichmentConfig, get_core_config
from ...core.exceptions import EmbeddingError
from ...payloads import (
    ClaimCellEmbeddingJobsPayload,
    EmbedClaimedCellPayload,
    EnqueueCellEmbeddingPayload,
    FailCellEmbeddingJobPayload,
    GetCellEmbeddingStatusPayload,
    GetEmbeddingCapabilityPayload,
)
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    resolve_tenant_id,
    validate_tenant_access,
    validate_tenant_write_policy,
    validate_token_for_read,
    validate_token_for_write,
)


class EmbeddingHandlersMixin(BrainHandlerBase):
    """gRPC handlers that keep provider calls and job state inside Brain."""

    def _embedding_config(self) -> EmbeddingEnrichmentConfig:
        return get_core_config().embedding_enrichment

    @grpc_error_handler
    async def GetEmbeddingCapability(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Report gate and storage readiness without invoking the embedder."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_EMBED)
        params = GetEmbeddingCapabilityPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        config = get_core_config()
        enabled = config.embedding_enrichment.enabled
        vector_backend_available = self.storage.vector_backend_available()
        ready = enabled and vector_backend_available
        reason_code: str | None = None
        if not enabled:
            reason_code = "embedding_disabled"
        elif not vector_backend_available:
            reason_code = "vector_backend_unavailable"
        payload: JsonDict = {
            "status": "ready" if ready else "unavailable",
            "enabled": enabled,
            "vector_backend_available": vector_backend_available,
            "profile": config.embeddings.space_id,
            "dimension": config.embeddings.dimension,
            "provider": config.embeddings.provider,
        }
        if reason_code is not None:
            payload["reason_code"] = reason_code
        return make_response(payload=payload, parent_unit=unit)

    @staticmethod
    def _reject_reserved(tenant_id: str) -> None:
        if tenant_id in {"_test", "_system"} or (tenant_id.startswith("_") and tenant_id != "_doc"):
            raise SecurityError(message="embedding jobs are not allowed for reserved tenants")

    @grpc_error_handler
    async def EnqueueCellEmbedding(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Durably enqueue a current cell hash without invoking a provider."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_WRITE)
        params = EnqueueCellEmbeddingPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        self._reject_reserved(tenant_id)
        cell = await self.storage.get_cell(tenant_id=tenant_id, cell_id=params.cell_id)
        if cell is None:
            return make_response(
                payload={"status": "rejected", "reason_code": "cell_not_found"}, parent_unit=unit
            )
        validate_tenant_write_policy(
            token,
            tenant_id,
            context,
            content=str(cell.get("content", "")),
            cell_kind=str(cell.get("cell_kind", "")),
            source_type=str(cell.get("source_type", "")),
        )
        cfg = self._embedding_config()
        if not cfg.enabled:
            return make_response(
                payload={"status": "rejected", "reason_code": "embedding_disabled"},
                parent_unit=unit,
            )
        profile = params.profile or get_core_config().embeddings.space_id
        if profile != get_core_config().embeddings.space_id:
            return make_response(
                payload={"status": "rejected", "reason_code": "profile_not_allowed"},
                parent_unit=unit,
            )
        result = await self.storage.enqueue_embedding_job(
            tenant_id=tenant_id,
            cell_id=params.cell_id,
            content_hash=params.content_hash,
            profile=profile,
            max_pending=cfg.max_pending_per_tenant,
        )
        return make_response(payload=result, parent_unit=unit)

    @grpc_error_handler
    async def ClaimCellEmbeddingJobs(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Lease reference-only jobs for a tenant-scoped Worker sweep."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_EMBED)
        params = ClaimCellEmbeddingJobsPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context, operation="read")
        cfg = self._embedding_config()
        if not cfg.enabled:
            return make_response(payload={"status": "skipped", "jobs": []}, parent_unit=unit)
        jobs = await self.storage.claim_embedding_jobs(
            tenant_id=tenant_id, limit=params.limit, lease_seconds=cfg.lease_seconds
        )
        return make_response(payload={"status": "claimed", "jobs": jobs}, parent_unit=unit)

    @grpc_error_handler
    async def EmbedClaimedCell(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Generate and persist one vector for a valid lease."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_EMBED)
        params = EmbedClaimedCellPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context, operation="read")
        cfg = self._embedding_config()
        if not cfg.enabled:
            return make_response(
                payload={"status": "skipped", "reason_code": "embedding_disabled"}, parent_unit=unit
            )
        job = await self.storage.get_embedding_job(
            tenant_id=tenant_id, job_id=params.job_id, lease_id=params.lease_id
        )
        if job is None:
            return make_response(
                payload={"status": "rejected", "reason_code": "stale_lease"}, parent_unit=unit
            )
        cell = await self.storage.get_cell(tenant_id=tenant_id, cell_id=str(job["cell_id"]))
        if cell is None or cell.get("content_hash") != job.get("content_hash"):
            await self.storage.mark_embedding_skipped(
                tenant_id=tenant_id,
                job_id=params.job_id,
                lease_id=params.lease_id,
                error_code="content_superseded",
            )
            return make_response(
                payload={"status": "skipped", "reason_code": "content_superseded"}, parent_unit=unit
            )
        content = cell.get("content")
        if not isinstance(content, str):
            return make_response(
                payload={"status": "retryable", "error_code": "content_missing"}, parent_unit=unit
            )
        if len(content) > cfg.max_input_chars:
            await self.storage.mark_embedding_skipped(
                tenant_id=tenant_id,
                job_id=params.job_id,
                lease_id=params.lease_id,
                error_code="input_too_large",
            )
            return make_response(
                payload={"status": "skipped", "reason_code": "input_too_large"}, parent_unit=unit
            )
        try:
            vector = await self.embedder.embed_async(content)
        except (EmbeddingError, OSError, RuntimeError, TimeoutError, ValueError):
            return make_response(
                payload={"status": "retryable", "error_code": "provider_failure"}, parent_unit=unit
            )
        expected_dim = get_core_config().embeddings.dimension
        if len(vector) != expected_dim:
            return make_response(
                payload={"status": "rejected", "error_code": "dimension_mismatch"},
                parent_unit=unit,
            )
        result = await self.storage.complete_embedding_job(
            tenant_id=tenant_id,
            job_id=params.job_id,
            lease_id=params.lease_id,
            vector=vector,
        )
        return make_response(payload=result, parent_unit=unit)

    @grpc_error_handler
    async def FailCellEmbeddingJob(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Record one terminal failure after Worker retry exhaustion."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context, required_permission=Permissions.BRAIN_EMBED)
        params = FailCellEmbeddingJobPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context, operation="read")
        result = await self.storage.terminal_fail_embedding_job(
            tenant_id=tenant_id,
            job_id=params.job_id,
            lease_id=params.lease_id,
            error_code=params.error_code,
        )
        return make_response(payload=result, parent_unit=unit)

    @grpc_error_handler
    async def GetCellEmbeddingStatus(
        self, request: contextunit_pb2.ContextUnit, context: grpc.ServicerContext
    ) -> contextunit_pb2.ContextUnit:
        """Read truthful status and vector presence for one cell."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context, required_permission=Permissions.BRAIN_READ)
        params = GetCellEmbeddingStatusPayload.model_validate(unit.payload or {})
        tenant_id = resolve_tenant_id(token, params.tenant_id)
        validate_tenant_access(token, tenant_id, context)
        profile = params.profile or get_core_config().embeddings.space_id
        if profile != get_core_config().embeddings.space_id:
            return make_response(
                payload={"status": "rejected", "reason_code": "profile_not_allowed"},
                parent_unit=unit,
            )
        result = await self.storage.get_embedding_status(
            tenant_id=tenant_id,
            cell_id=params.cell_id,
            content_hash=params.content_hash,
            profile=profile,
        )
        return make_response(payload=result, parent_unit=unit)


__all__ = ["EmbeddingHandlersMixin"]
