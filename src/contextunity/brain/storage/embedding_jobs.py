"""Backend-neutral value objects for durable cell embedding work."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from contextunity.core.types import JsonDict

EmbeddingJobStatus = Literal["pending", "processing", "ready", "failed", "skipped"]
EmbeddingTransitionStatus = Literal["pending", "failed", "skipped"]


def embedding_job_status_counts(rows: Iterable[JsonDict]) -> JsonDict:
    """Project backend aggregate rows onto the stable admin status shape."""
    counts: JsonDict = {
        "pending": 0,
        "processing": 0,
        "ready": 0,
        "failed": 0,
        "skipped": 0,
    }
    for row in rows:
        status = row.get("status")
        count = row.get("count")
        if isinstance(status, str) and status in counts and isinstance(count, int):
            counts[status] = count
    return counts


def first_row(rows: list[JsonDict]) -> JsonDict | None:
    """Return the one expected row without leaking positional access to callers."""
    return next(iter(rows), None)


def embedding_idempotency_key(
    *, tenant_id: str, cell_id: str, content_hash: str, profile: str
) -> str:
    """Derive the server-owned idempotency boundary for one cell revision."""
    return f"{tenant_id}:{cell_id}:{content_hash}:{profile}"


def embedding_metadata(
    *,
    profile: str,
    content_hash: str,
    attempt: int,
    status: EmbeddingJobStatus,
    error_code: str | None = None,
) -> JsonDict:
    """Project content-free lifecycle state into the current BrainCell."""
    metadata: JsonDict = {
        "embedding_status": status,
        "embedding_profile": profile,
        "embedding_content_hash": content_hash,
        "embedding_attempt": attempt,
    }
    if error_code:
        metadata["embedding_error_code"] = error_code
    return metadata


def embedding_transition_result(
    *,
    status: EmbeddingTransitionStatus,
    attempt: int,
    error_code: str,
    idempotent: bool = False,
) -> JsonDict:
    """Return the backend-independent result for one leased-job transition."""
    if status == "pending":
        return {"status": status, "retryable": True, "attempt": attempt}
    if status == "skipped":
        return {"status": status, "reason_code": error_code}
    result: JsonDict = {"status": status, "attempt": attempt}
    if idempotent:
        result["idempotent"] = True
    return result


__all__ = [
    "EmbeddingJobStatus",
    "EmbeddingTransitionStatus",
    "embedding_idempotency_key",
    "embedding_job_status_counts",
    "embedding_metadata",
    "embedding_transition_result",
    "first_row",
]
