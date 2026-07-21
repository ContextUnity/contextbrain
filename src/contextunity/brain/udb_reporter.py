"""Brain-local, non-recursive UDB reporting for proven negative paths."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from json import dumps as canonical_dumps
from uuid import NAMESPACE_URL, uuid5

from contextunity.core import get_contextunit_logger
from contextunity.core.udb import FaultOccurrence, UdbComparisonKey, udb_fingerprint

from contextunity.brain.core.exceptions import UdbFeatureDisabledError
from contextunity.brain.udb_application import UdbApplication

logger = get_contextunit_logger(__name__)


class BrainUdbReporter:
    """Direct Brain-local reporter with sanitized, non-recursive failure handling."""

    def __init__(self, *, application: UdbApplication) -> None:
        self._application = application

    async def report_embedding_provider_failure(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
    ) -> None:
        """Record one failed embedding provider attempt without embedding content."""
        identity = uuid5(
            NAMESPACE_URL,
            canonical_dumps(
                {
                    "job_id": job_id,
                    "lease_id": lease_id,
                    "operation_kind": "embedding_enrichment",
                    "tenant_id": tenant_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        subject_ref = f"embedding-job:{sha256(job_id.encode('utf-8')).hexdigest()}"
        comparison_key = UdbComparisonKey(
            tenant_id=tenant_id,
            operation_kind="embedding_enrichment",
            subject_ref=subject_ref,
            capability_class="brain:embed",
        )
        occurrence = FaultOccurrence(
            occurrence_id=identity,
            tenant_id=tenant_id,
            producer_id="brain:embedding-enrichment",
            idempotency_key=str(identity),
            fingerprint_version="contextunity.udb-fingerprint/v1",
            fingerprint=udb_fingerprint(
                fault_code="brain.embedding.provider_failure",
                comparison_key=comparison_key,
            ),
            fault_class="upstream_fault",
            operation_kind="embedding_enrichment",
            fault_code="brain.embedding.provider_failure",
            comparison_key=comparison_key,
            occurred_at=datetime.now(UTC),
        )
        try:
            await self._application.report_fault_occurrence(occurrence)
        except UdbFeatureDisabledError:
            return
        except Exception:
            # UDB is the failed backend; do not feed this failure back into it.
            logger.warning(
                "Brain UDB local delivery failed: producer=%s fault_code=%s",
                occurrence.producer_id,
                occurrence.fault_code,
            )


__all__ = ["BrainUdbReporter"]
