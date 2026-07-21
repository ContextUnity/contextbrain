"""Behavioral checks for durable reference-only queue state."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
from contextunity.core import ContextUnit, contextunit_pb2

from contextunity.brain.service.handlers.embedding import EmbeddingHandlersMixin
from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.fixture
def store(tmp_path: Path) -> SqliteBrainStore:
    """Create an isolated local store."""
    return SqliteBrainStore(db_path=tmp_path / "state.sqlite3", vector_dim=2)


@pytest.mark.asyncio
async def test_duplicate_submission_keeps_one_job(store: SqliteBrainStore) -> None:
    """The same cell hash/profile returns the original job."""
    cell = await store.upsert_cell(
        tenant_id="tenant-a",
        cell_kind="document",
        content="bounded content",
        content_hash="sha256:one",
        source_type="manual",
    )
    first = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=str(cell["id"]),
        content_hash="sha256:one",
        profile="default",
        max_pending=10,
    )
    second = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=str(cell["id"]),
        content_hash="sha256:one",
        profile="default",
        max_pending=10,
    )
    assert first["accepted"] is True
    assert second["accepted"] is False
    assert second["job_id"] == first["job_id"]


@pytest.mark.asyncio
async def test_expired_lease_can_be_reclaimed(store: SqliteBrainStore) -> None:
    """A processing lease is recoverable after its deadline."""
    cell = await store.upsert_cell(
        tenant_id="tenant-a",
        cell_kind="document",
        content="reclaim me",
        content_hash="sha256:two",
        source_type="manual",
    )
    job = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=str(cell["id"]),
        content_hash="sha256:two",
        profile="default",
        max_pending=10,
    )
    first = await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=1)
    first_job = next(iter(first), None)
    assert first_job is not None and first_job["job_id"] == job["job_id"]
    conn = store.get_sqlite_connection()
    conn.execute(
        "UPDATE cell_embedding_jobs SET lease_until = ? WHERE job_id = ?",
        ("2000-01-01T00:00:00+00:00", job["job_id"]),
    )
    conn.commit()
    conn.close()
    second = await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=1)
    second_job = next(iter(second), None)
    assert second_job is not None and second_job["job_id"] == job["job_id"]
    assert second_job["lease_id"] != first_job["lease_id"]


@pytest.mark.asyncio
async def test_missing_vector_backend_is_truthful(store: SqliteBrainStore) -> None:
    """Local mode never reports ready when vector storage is unavailable."""
    cell = await store.upsert_cell(
        tenant_id="tenant-a",
        cell_kind="document",
        content="no vector extension",
        content_hash="sha256:three",
        source_type="manual",
    )
    await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=str(cell["id"]),
        content_hash="sha256:three",
        profile="default",
        max_pending=10,
    )
    jobs = await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=60)
    job = next(iter(jobs), None)
    assert job is not None
    result = await store.complete_embedding_job(
        tenant_id="tenant-a",
        job_id=str(job["job_id"]),
        lease_id=str(job["lease_id"]),
        vector=[0.1, 0.2],
    )
    if not store.has_sqlite_vec():
        assert result["status"] == "skipped"


@pytest.mark.asyncio
async def test_superseded_content_closes_lease_without_overwriting_current_metadata(
    store: SqliteBrainStore,
) -> None:
    """Atomic completion skips obsolete work and does not leave a processing lease."""
    cell_id = str(uuid4())
    await store.upsert_cell(
        tenant_id="tenant-a",
        cell_id=cell_id,
        cell_kind="document",
        content="old revision",
        content_hash="sha256:old",
        source_type="manual",
    )
    accepted = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=cell_id,
        content_hash="sha256:old",
        profile="default",
        max_pending=10,
    )
    job = next(
        iter(await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=60)),
        None,
    )
    assert job is not None
    await store.upsert_cell(
        tenant_id="tenant-a",
        cell_id=cell_id,
        cell_kind="document",
        content="new revision",
        content_hash="sha256:new",
        source_type="manual",
    )

    result = await store.complete_embedding_job(
        tenant_id="tenant-a",
        job_id=str(job["job_id"]),
        lease_id=str(job["lease_id"]),
        vector=[0.1, 0.2],
    )

    assert result == {"status": "skipped", "reason_code": "content_superseded"}
    status = await store.get_embedding_status(
        tenant_id="tenant-a",
        cell_id=cell_id,
        content_hash="sha256:old",
        profile="default",
    )
    assert status["status"] == "skipped"
    assert await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=60) == []
    current = await store.get_cell(tenant_id="tenant-a", cell_id=cell_id)
    assert current is not None
    assert current["content_hash"] == "sha256:new"
    assert current.get("embedding_status") != "skipped"

    await store.upsert_cell(
        tenant_id="tenant-a",
        cell_id=cell_id,
        cell_kind="document",
        content="old revision",
        content_hash="sha256:old",
        source_type="manual",
    )
    requeued = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=cell_id,
        content_hash="sha256:old",
        profile="default",
        max_pending=10,
    )
    assert requeued == {
        "job_id": job["job_id"],
        "status": "pending",
        "idempotency_key": accepted["idempotency_key"],
        "accepted": True,
        "requeued": True,
    }
    claimed_again = await store.claim_embedding_jobs(
        tenant_id="tenant-a", limit=1, lease_seconds=60
    )
    retried_job = next(iter(claimed_again), None)
    assert retried_job is not None
    assert retried_job["job_id"] == job["job_id"]
    assert retried_job["attempt"] == 1


@pytest.mark.asyncio
async def test_retry_and_terminal_paths_preserve_distinct_states(store: SqliteBrainStore) -> None:
    """Retry, skip, and terminal failure are not aliases of one another."""
    cell = await store.upsert_cell(
        tenant_id="tenant-a",
        cell_kind="document",
        content="transition state",
        content_hash="sha256:four",
        source_type="manual",
    )
    accepted = await store.enqueue_embedding_job(
        tenant_id="tenant-a",
        cell_id=str(cell["id"]),
        content_hash="sha256:four",
        profile="default",
        max_pending=10,
    )
    first = next(
        iter(await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=60)),
        None,
    )
    assert first is not None
    retry = await store.fail_embedding_job(
        tenant_id="tenant-a",
        job_id=str(first["job_id"]),
        lease_id=str(first["lease_id"]),
        error_code="provider_failure",
    )
    assert retry == {"status": "pending", "retryable": True, "attempt": 1}
    second = next(
        iter(await store.claim_embedding_jobs(tenant_id="tenant-a", limit=1, lease_seconds=60)),
        None,
    )
    assert second is not None and second["job_id"] == accepted["job_id"]
    failed = await store.terminal_fail_embedding_job(
        tenant_id="tenant-a",
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert failed == {"status": "failed", "attempt": 2}
    repeated = await store.terminal_fail_embedding_job(
        tenant_id="tenant-a",
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert repeated == {"status": "failed", "attempt": 2, "idempotent": True}


@pytest.mark.asyncio
async def test_live_backend_preserves_reference_lifecycle() -> None:
    """The live backend stores and leases references under tenant RLS."""
    dsn = os.environ.get("BRAIN_TEST_DSN")
    if not dsn:
        pytest.skip("BRAIN_TEST_DSN not set")
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=dsn, schema="brain")
    await store.ensure_schema()
    tenant_id = f"queue-contract-{uuid4().hex}"
    content_hash = f"sha256:{uuid4()}"
    cell = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_kind="document",
        content=f"live reference {uuid4()}",
        content_hash=content_hash,
        source_type="manual",
    )
    accepted = await store.enqueue_embedding_job(
        tenant_id=tenant_id,
        cell_id=str(cell["id"]),
        content_hash=content_hash,
        profile="default",
        max_pending=10,
    )
    jobs = await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60)
    job = next(iter(jobs), None)
    assert accepted["status"] == "pending"
    assert job is not None and job["job_id"] == accepted["job_id"]
    completed = await store.complete_embedding_job(
        tenant_id=tenant_id,
        job_id=str(job["job_id"]),
        lease_id=str(job["lease_id"]),
        vector=[0.0] * 768,
    )
    assert completed["status"] == "ready"
    status = await store.get_embedding_status(
        tenant_id=tenant_id,
        cell_id=str(cell["id"]),
        content_hash=content_hash,
        profile="default",
    )
    assert status["status"] == "ready"
    assert status["vector_present"] is True

    superseded_cell_id = str(uuid4())
    await store.upsert_cell(
        tenant_id=tenant_id,
        cell_id=superseded_cell_id,
        cell_kind="document",
        content="old live revision",
        content_hash="sha256:old-live",
        source_type="manual",
    )
    await store.enqueue_embedding_job(
        tenant_id=tenant_id,
        cell_id=superseded_cell_id,
        content_hash="sha256:old-live",
        profile="default",
        max_pending=10,
    )
    superseded_job = next(
        iter(await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60)),
        None,
    )
    assert superseded_job is not None
    await store.upsert_cell(
        tenant_id=tenant_id,
        cell_id=superseded_cell_id,
        cell_kind="document",
        content="new live revision",
        content_hash="sha256:new-live",
        source_type="manual",
    )
    superseded_result = await store.complete_embedding_job(
        tenant_id=tenant_id,
        job_id=str(superseded_job["job_id"]),
        lease_id=str(superseded_job["lease_id"]),
        vector=[0.0] * 768,
    )
    assert superseded_result == {
        "status": "skipped",
        "reason_code": "content_superseded",
    }
    await store.upsert_cell(
        tenant_id=tenant_id,
        cell_id=superseded_cell_id,
        cell_kind="document",
        content="old live revision",
        content_hash="sha256:old-live",
        source_type="manual",
    )
    requeued = await store.enqueue_embedding_job(
        tenant_id=tenant_id,
        cell_id=superseded_cell_id,
        content_hash="sha256:old-live",
        profile="default",
        max_pending=10,
    )
    assert requeued["job_id"] == superseded_job["job_id"]
    assert requeued["status"] == "pending"
    assert requeued["accepted"] is True
    assert requeued["requeued"] is True
    await store.close()


class _EmbeddingStorage:
    """Minimal handler seam proving terminal classification before storage writes."""

    async def get_embedding_job(self, **_: object) -> dict[str, object]:
        return {
            "cell_id": "cell-1",
            "content_hash": "sha256:one",
            "profile": "default",
            "attempt": 1,
            "status": "processing",
        }

    async def get_cell(self, **_: object) -> dict[str, object]:
        return {"content": "stored content", "content_hash": "sha256:one"}

    async def complete_embedding_job(self, **_: object) -> dict[str, object]:
        raise AssertionError("dimension mismatch must not write a vector")

    async def mark_embedding_skipped(self, **_: object) -> dict[str, object]:
        raise AssertionError("this test has current cell content")

    def vector_backend_available(self) -> bool:
        return True


class _EmbeddingService(EmbeddingHandlersMixin):
    def __init__(self) -> None:
        self.storage = _EmbeddingStorage()
        self.embedder = type("Embedder", (), {"embed_document_async": self._embed})()

    async def _embed(self, _: str) -> list[float]:
        return [0.1]


@pytest.mark.asyncio
async def test_dimension_mismatch_is_returned_as_typed_terminal_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Brain exposes mismatch to Worker, which records the terminal state once."""
    from types import SimpleNamespace

    from contextunity.brain.service.handlers import embedding as handler

    service = _EmbeddingService()
    monkeypatch.setattr(handler, "extract_token_from_context", lambda _: object())
    monkeypatch.setattr(handler, "validate_token_for_write", lambda *args, **kwargs: None)
    monkeypatch.setattr(handler, "resolve_tenant_id", lambda *_: "tenant-a")
    monkeypatch.setattr(handler, "validate_tenant_access", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        handler,
        "get_core_config",
        lambda: SimpleNamespace(
            embedding_enrichment=SimpleNamespace(enabled=True, max_input_chars=100),
            embeddings=SimpleNamespace(dimension=2),
        ),
    )
    request = ContextUnit(
        payload={"tenant_id": "tenant-a", "job_id": "job-1", "lease_id": "lease-1"}
    ).to_protobuf(contextunit_pb2)

    response = await service.EmbedClaimedCell(request, SimpleNamespace())

    assert ContextUnit.from_protobuf(response).payload == {
        "status": "rejected",
        "error_code": "dimension_mismatch",
    }


@pytest.mark.asyncio
async def test_provider_failure_reports_through_local_udb_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from contextunity.brain.service.handlers import embedding as handler

    reported: list[dict[str, str]] = []

    class _Reporter:
        def __init__(self, *, application: object) -> None:
            del application

        async def report_embedding_provider_failure(self, **kwargs: str) -> None:
            reported.append(kwargs)

    class _FailingEmbedder:
        async def embed_document_async(self, _: str) -> list[float]:
            raise OSError("provider unavailable")

    service = _EmbeddingService()
    service.embedder = _FailingEmbedder()
    monkeypatch.setattr(handler, "BrainUdbReporter", _Reporter)
    monkeypatch.setattr(handler, "extract_token_from_context", lambda _: object())
    monkeypatch.setattr(handler, "validate_token_for_write", lambda *args, **kwargs: None)
    monkeypatch.setattr(handler, "resolve_tenant_id", lambda *_: "tenant-a")
    monkeypatch.setattr(handler, "validate_tenant_access", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        handler,
        "get_core_config",
        lambda: SimpleNamespace(
            embedding_enrichment=SimpleNamespace(enabled=True, max_input_chars=100),
            embeddings=SimpleNamespace(dimension=2),
            udb=SimpleNamespace(enabled=True),
        ),
    )
    request = ContextUnit(
        payload={"tenant_id": "tenant-a", "job_id": "job-1", "lease_id": "lease-1"}
    ).to_protobuf(contextunit_pb2)

    response = await service.EmbedClaimedCell(request, SimpleNamespace())

    assert ContextUnit.from_protobuf(response).payload == {
        "status": "retryable",
        "error_code": "provider_failure",
    }
    assert reported == [{"tenant_id": "tenant-a", "job_id": "job-1", "lease_id": "lease-1"}]


@pytest.mark.asyncio
async def test_embedding_capability_reports_gate_and_storage_without_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from contextunity.brain.service.handlers import embedding as handler

    service = _EmbeddingService()
    monkeypatch.setattr(handler, "extract_token_from_context", lambda _: object())
    monkeypatch.setattr(handler, "validate_token_for_read", lambda *args, **kwargs: None)
    monkeypatch.setattr(handler, "resolve_tenant_id", lambda *_: "tenant-a")
    monkeypatch.setattr(handler, "validate_tenant_access", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        handler,
        "get_core_config",
        lambda: SimpleNamespace(
            embedding_enrichment=SimpleNamespace(enabled=True),
            embeddings=SimpleNamespace(
                dimension=2,
                space_id="test-space",
                provider="deterministic",
            ),
        ),
    )
    request = ContextUnit(payload={"tenant_id": "tenant-a"}).to_protobuf(contextunit_pb2)

    response = await service.GetEmbeddingCapability(request, SimpleNamespace())

    assert ContextUnit.from_protobuf(response).payload == {
        "status": "ready",
        "enabled": True,
        "vector_backend_available": True,
        "profile": "test-space",
        "dimension": 2,
        "provider": "deterministic",
    }
