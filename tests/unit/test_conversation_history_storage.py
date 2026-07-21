"""One Conversation History contract suite for SQLite and live PostgreSQL."""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from contextunity.core.sdk.conversation import (
    ConversationAppendReceipt,
    ConversationHistoryStats,
    ConversationKind,
    ConversationProjection,
    ConversationRecord,
    ConversationRetentionReceipt,
    ConversationRole,
    conversation_content_hash,
    conversation_retention_evidence_hash,
)
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.sqlite import SqliteBrainStore

RECORD_ID = UUID("11111111-1111-4111-8111-111111111111")
RUN_ID = UUID("22222222-2222-4222-8222-222222222222")
CONTENT = "hello"
CONTENT_HASH = "sha256:" + hashlib.sha256(CONTENT.encode()).hexdigest()
SOURCE_HASH = "sha256:" + "b" * 64
BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")


class ConversationStore(Protocol):
    async def append_conversation_record(
        self,
        *,
        record_id: UUID,
        tenant_id: str,
        user_id: str,
        session_id: str | None,
        role: ConversationRole,
        kind: ConversationKind,
        content: str,
        content_hash: str,
        source_hash: str,
        graph_run_id: UUID | None,
        metadata_version: int,
        idempotency_key: str,
        metadata: JsonDict,
        created_at: datetime | None = None,
    ) -> ConversationAppendReceipt: ...

    async def query_conversation_history(
        self,
        *,
        tenant_id: str,
        projection: ConversationProjection,
        user_id: str | None,
        session_id: str | None,
        graph_run_id: UUID | None,
        older_than_days: int | None,
        limit: int,
        offset: int,
    ) -> list[ConversationRecord]: ...

    async def get_conversation_history_stats(
        self, *, tenant_id: str
    ) -> ConversationHistoryStats: ...

    async def apply_conversation_retention(
        self,
        *,
        tenant_id: str,
        record_ids: list[UUID],
        cutoff: datetime,
        policy_version: Literal["contextunity.conversation-retention/v1"],
        hold_evidence_hash: str,
    ) -> ConversationRetentionReceipt: ...


@pytest_asyncio.fixture(params=("sqlite", "postgres"))
async def backend_store(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AsyncIterator[ConversationStore]:
    if request.param == "sqlite":
        yield SqliteBrainStore(str(tmp_path / "brain.sqlite3"))
        return
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — live Postgres parity is unavailable")
    from psycopg import AsyncConnection, sql

    from contextunity.brain.storage.postgres import PostgresBrainStore

    schema = f"conversation_parity_{uuid4().hex}"
    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN, schema=schema)
    await store.ensure_schema()
    try:
        yield store
    finally:
        await store.close()
        admin = await AsyncConnection.connect(BRAIN_TEST_DSN, autocommit=True)
        try:
            await admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
        finally:
            await admin.close()


async def _append(
    store: ConversationStore,
    *,
    record_id: UUID = RECORD_ID,
    user_id: str = "user-a",
    session_id: str | None = "session-a",
    role: ConversationRole = "user",
    kind: ConversationKind = "message",
    content: str = CONTENT,
    content_hash: str = CONTENT_HASH,
    source_hash: str = SOURCE_HASH,
    graph_run_id: UUID | None = RUN_ID,
    metadata: JsonDict | None = None,
    created_at: datetime | None = None,
) -> ConversationAppendReceipt:
    return await store.append_conversation_record(
        record_id=record_id,
        tenant_id="tenant-a",
        user_id=user_id,
        session_id=session_id,
        role=role,
        kind=kind,
        content=content,
        content_hash=content_hash,
        source_hash=source_hash,
        graph_run_id=graph_run_id,
        metadata_version=1,
        idempotency_key="router:session-a:turn-1",
        metadata=metadata if metadata is not None else {"source": "router"},
        created_at=created_at,
    )


async def _append_record(
    store: ConversationStore,
    *,
    record_id: UUID,
    created_at: datetime,
    user_id: str,
    session_id: str,
    graph_run_id: UUID,
    key: str,
    content: str,
) -> None:
    await store.append_conversation_record(
        record_id=record_id,
        tenant_id="tenant-a",
        user_id=user_id,
        session_id=session_id,
        role="assistant",
        kind="conversation_note",
        content=content,
        content_hash=conversation_content_hash(content),
        source_hash="sha256:" + record_id.hex * 2,
        graph_run_id=graph_run_id,
        metadata_version=1,
        idempotency_key=key,
        metadata={"ordinal": key},
        created_at=created_at,
    )


@pytest.mark.asyncio
async def test_append_duplicate_and_conflict_are_durable(
    backend_store: ConversationStore,
) -> None:
    created = await _append(backend_store)
    duplicate = await _append(backend_store)

    assert created.outcome == "created"
    assert duplicate == created.model_copy(update={"outcome": "duplicate"})
    conflicting_retries: tuple[Callable[[], Awaitable[ConversationAppendReceipt]], ...] = (
        lambda: _append(backend_store, record_id=uuid4()),
        lambda: _append(backend_store, user_id="user-b"),
        lambda: _append(backend_store, session_id="session-b"),
        lambda: _append(backend_store, role="assistant"),
        lambda: _append(backend_store, kind="conversation_note"),
        lambda: _append(
            backend_store,
            content="changed",
            content_hash=conversation_content_hash("changed"),
        ),
        lambda: _append(backend_store, source_hash="sha256:" + "c" * 64),
        lambda: _append(backend_store, graph_run_id=uuid4()),
        lambda: _append(backend_store, metadata={"source": "different"}),
        lambda: _append(backend_store, created_at=datetime(2026, 1, 1, tzinfo=UTC)),
    )
    for retry in conflicting_retries:
        with pytest.raises(BrainValidationError, match="idempotency key conflicts"):
            await retry()


@pytest.mark.asyncio
async def test_query_projections_are_tenant_scoped_and_typed(
    backend_store: ConversationStore,
) -> None:
    await _append(backend_store)

    records = await backend_store.query_conversation_history(
        tenant_id="tenant-a",
        projection="recent",
        user_id="user-a",
        session_id=None,
        graph_run_id=None,
        older_than_days=None,
        limit=10,
        offset=0,
    )
    other_tenant = await backend_store.query_conversation_history(
        tenant_id="tenant-b",
        projection="recent",
        user_id="user-a",
        session_id=None,
        graph_run_id=None,
        older_than_days=None,
        limit=10,
        offset=0,
    )

    assert records[0].record_id == RECORD_ID
    assert records[0].graph_run_id == RUN_ID
    assert records[0].content_hash == CONTENT_HASH
    assert other_tenant == []


@pytest.mark.asyncio
async def test_projection_pagination_stats_and_retention_match(
    backend_store: ConversationStore,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    old_id = UUID("33333333-3333-4333-8333-333333333333")
    middle_id = UUID("44444444-4444-4444-8444-444444444444")
    latest_id = UUID("55555555-5555-4555-8555-555555555555")
    shared_run = UUID("66666666-6666-4666-8666-666666666666")
    await _append_record(
        backend_store,
        record_id=old_id,
        created_at=now - timedelta(days=40),
        user_id="user-a",
        session_id="session-old",
        graph_run_id=shared_run,
        key="old",
        content="old",
    )
    await _append_record(
        backend_store,
        record_id=middle_id,
        created_at=now - timedelta(days=1),
        user_id="user-a",
        session_id="session-current",
        graph_run_id=shared_run,
        key="middle",
        content="middle",
    )
    await _append_record(
        backend_store,
        record_id=latest_id,
        created_at=now,
        user_id="user-a",
        session_id="session-current",
        graph_run_id=uuid4(),
        key="latest",
        content="latest",
    )

    recent_page = await backend_store.query_conversation_history(
        tenant_id="tenant-a",
        projection="recent",
        user_id="user-a",
        session_id=None,
        graph_run_id=None,
        older_than_days=None,
        limit=1,
        offset=1,
    )
    session_records = await backend_store.query_conversation_history(
        tenant_id="tenant-a",
        projection="session",
        user_id=None,
        session_id="session-current",
        graph_run_id=None,
        older_than_days=None,
        limit=10,
        offset=0,
    )
    trace_records = await backend_store.query_conversation_history(
        tenant_id="tenant-a",
        projection="trace_related",
        user_id=None,
        session_id=None,
        graph_run_id=shared_run,
        older_than_days=None,
        limit=10,
        offset=0,
    )
    old_records = await backend_store.query_conversation_history(
        tenant_id="tenant-a",
        projection="older_than",
        user_id=None,
        session_id=None,
        graph_run_id=None,
        older_than_days=30,
        limit=10,
        offset=0,
    )
    stats = await backend_store.get_conversation_history_stats(tenant_id="tenant-a")

    assert [record.record_id for record in recent_page] == [middle_id]
    assert [record.record_id for record in session_records] == [latest_id, middle_id]
    assert [record.record_id for record in trace_records] == [middle_id, old_id]
    assert [record.record_id for record in old_records] == [old_id]
    assert stats.total == 3
    assert stats.oldest == now - timedelta(days=40)
    assert stats.newest == now

    cutoff = now - timedelta(days=30)
    evidence_hash = conversation_retention_evidence_hash(
        tenant_id="tenant-a", cutoff=cutoff, record_ids=[old_id, middle_id]
    )
    receipt = await backend_store.apply_conversation_retention(
        tenant_id="tenant-a",
        record_ids=[old_id, middle_id],
        cutoff=cutoff,
        policy_version="contextunity.conversation-retention/v1",
        hold_evidence_hash=evidence_hash,
    )

    assert receipt.deleted_count == 1
    assert receipt.hold_evidence_hash == evidence_hash
    assert (await backend_store.get_conversation_history_stats(tenant_id="tenant-a")).total == 2
