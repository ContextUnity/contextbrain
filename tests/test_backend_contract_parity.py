"""Shared business-contract checks for SQLite and PostgreSQL stores."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from contextunity.brain.storage.contracts import BrainStorageProtocol
from contextunity.brain.storage.postgres import PostgresBrainStore
from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)


@pytest_asyncio.fixture
async def postgres_store() -> PostgresBrainStore:
    dsn = (os.environ.get("BRAIN_TEST_DSN") or "").strip()
    if not dsn:
        pytest.skip("BRAIN_TEST_DSN not set")
    store = PostgresBrainStore(dsn=dsn, schema="brain")
    await store.ensure_schema()
    try:
        yield store
    finally:
        await store.close()


async def _assert_cell_contract(store: BrainStorageProtocol, *, tenant_id: str) -> None:
    upserted = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="A stable backend contract",
        metadata={"domain": "parity"},
        scope_path=f"{tenant_id}.memory",
        content_hash=f"sha256:{uuid4().hex}",
        source_type="auto_extract",
        source_ref="episode-1",
        confidence=0.99,
        visibility="tenant",
    )
    assert set(upserted) == {
        "id",
        "tenant_id",
        "cell_kind",
        "source_type",
        "scope_path",
        "content_hash",
        "confidence",
        "visibility",
        "created_at",
        "updated_at",
    }
    assert upserted["confidence"] == 0.75

    rows = await store.query_cells(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        source_type="auto_extract",
        scope_path=f"{tenant_id}.memory",
        limit=10,
    )
    assert len(rows) == 1
    assert set(rows[0]) == {
        "id",
        "tenant_id",
        "cell_kind",
        "content",
        "metadata",
        "content_hash",
        "scope_path",
        "source_type",
        "source_ref",
        "confidence",
        "visibility",
    }
    assert rows[0]["id"] == upserted["id"]

    cell = await store.get_cell(
        tenant_id=tenant_id,
        cell_id=str(upserted["id"]),
        user_id="user-a",
    )
    assert cell is not None
    assert set(cell) == {
        "id",
        "tenant_id",
        "cell_kind",
        "content",
        "metadata",
        "content_hash",
        "source_type",
        "source_ref",
        "scope_path",
        "confidence",
        "visibility",
        "created_at",
        "updated_at",
    }
    assert (
        await store.get_cell(
            tenant_id=tenant_id,
            cell_id=str(upserted["id"]),
            user_id="user-b",
        )
        is None
    )

    source_owned = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="Source owned identity",
        source_type="auto_extract",
    )
    normalized_repeat = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content=" Source   owned\nidentity ",
        source_type="auto_extract",
    )
    other_producer = await store.upsert_cell(
        tenant_id=tenant_id,
        user_id="user-a",
        cell_kind="fact",
        content="Source owned identity",
        source_type="synthesis",
    )
    assert normalized_repeat["id"] == source_owned["id"]
    assert normalized_repeat["content_hash"] == source_owned["content_hash"]
    assert other_producer["id"] != source_owned["id"]
    assert other_producer["content_hash"] != source_owned["content_hash"]


async def _assert_embedding_transition_contract(
    store: BrainStorageProtocol,
    *,
    tenant_id: str,
) -> None:
    content_hash = f"sha256:{uuid4().hex}"
    cell = await store.upsert_cell(
        tenant_id=tenant_id,
        cell_kind="document",
        content="Embedding transition contract",
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
    assert accepted["status"] == "pending"
    assert accepted["accepted"] is True

    first = (await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60))[0]
    retry = await store.fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(first["job_id"]),
        lease_id=str(first["lease_id"]),
        error_code="provider_failure",
    )
    assert retry == {"status": "pending", "retryable": True, "attempt": 1}

    second = (await store.claim_embedding_jobs(tenant_id=tenant_id, limit=1, lease_seconds=60))[0]
    failed = await store.terminal_fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert failed == {"status": "failed", "attempt": 2}
    repeated = await store.terminal_fail_embedding_job(
        tenant_id=tenant_id,
        job_id=str(second["job_id"]),
        lease_id=str(second["lease_id"]),
        error_code="provider_failure",
    )
    assert repeated == {"status": "failed", "attempt": 2, "idempotent": True}


@pytest.mark.asyncio
async def test_sqlite_business_contract(sqlite_store: SqliteBrainStore) -> None:
    tenant_id = f"sqlite-parity-{uuid4().hex}"
    await _assert_cell_contract(sqlite_store, tenant_id=tenant_id)
    await _assert_embedding_transition_contract(sqlite_store, tenant_id=tenant_id)


@pytest.mark.asyncio
async def test_postgres_business_contract(postgres_store: PostgresBrainStore) -> None:
    tenant_id = f"postgres-parity-{uuid4().hex}"
    await _assert_cell_contract(postgres_store, tenant_id=tenant_id)
    await _assert_embedding_transition_contract(postgres_store, tenant_id=tenant_id)
