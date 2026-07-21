"""Conversation History handler authorization and evidence tests."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from contextunity.core import ContextToken, ContextUnit, contextunit_pb2
from contextunity.core.permissions import Permissions
from contextunity.core.sdk.conversation import (
    ConversationAppendReceipt,
    ConversationHistoryStats,
    ConversationRecord,
    ConversationRetentionReceipt,
    conversation_content_hash,
    conversation_retention_evidence_hash,
)
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.service.handlers.memory import MemoryHandlersMixin

RECORD_ID = UUID("11111111-1111-4111-8111-111111111111")


class FakeConversationStorage:
    def __init__(self) -> None:
        self.calls: list[JsonDict] = []

    async def append_conversation_record(self, **kwargs: object) -> ConversationAppendReceipt:
        self.calls.append(dict(kwargs))
        return ConversationAppendReceipt(
            record_id=RECORD_ID,
            outcome="created",
            content_hash=conversation_content_hash("hello"),
            source_hash="sha256:" + "b" * 64,
        )

    async def query_conversation_history(self, **kwargs: object) -> list[ConversationRecord]:
        self.calls.append(dict(kwargs))
        return [
            ConversationRecord(
                record_id=RECORD_ID,
                tenant_id="tenant-a",
                user_id="user-1",
                session_id=None,
                role="user",
                kind="message",
                content="hello",
                content_hash="sha256:" + "a" * 64,
                source_hash="sha256:" + "b" * 64,
                graph_run_id=None,
                created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                metadata_version=1,
                idempotency_key="test:1",
                metadata={},
            )
        ]

    async def get_conversation_history_stats(self, *, tenant_id: str) -> ConversationHistoryStats:
        return ConversationHistoryStats(tenant_id=tenant_id, total=1)

    async def apply_conversation_retention(
        self, *, tenant_id: str, policy_version: str, hold_evidence_hash: str, **_: object
    ) -> ConversationRetentionReceipt:
        self.calls.append({"tenant_id": tenant_id, "hold_evidence_hash": hold_evidence_hash})
        return ConversationRetentionReceipt(
            tenant_id=tenant_id,
            deleted_count=1,
            policy_version=policy_version,
            hold_evidence_hash=hold_evidence_hash,
        )


class MemoryServiceForTest(MemoryHandlersMixin):
    def __init__(self, storage: FakeConversationStorage) -> None:
        self.storage = storage


def _token(*permissions: str) -> ContextToken:
    return ContextToken(
        token_id="test",
        permissions=permissions,
        allowed_tenants=("tenant-a",),
        user_id="user-1",
    )


@pytest.mark.asyncio
async def test_append_conversation_record_validates_and_calls_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import memory as handler_module

    storage = FakeConversationStorage()
    service = MemoryServiceForTest(storage)
    monkeypatch.setattr(
        handler_module,
        "extract_token_from_context",
        lambda _context: _token(Permissions.MEMORY_WRITE),
    )
    unit = ContextUnit(
        payload={
            "record_id": str(RECORD_ID),
            "tenant_id": "tenant-a",
            "user_id": "user-1",
            "session_id": "session-a",
            "role": "user",
            "kind": "message",
            "content": "hello",
            "content_hash": conversation_content_hash("hello"),
            "source_hash": "sha256:" + "b" * 64,
            "graph_run_id": None,
            "metadata_version": 1,
            "idempotency_key": "test:1",
            "metadata": {},
        }
    )

    response = await service.AppendConversationRecord(
        unit.to_protobuf(contextunit_pb2), SimpleNamespace()
    )
    payload = ContextUnit.from_protobuf(response).payload

    assert payload["record_id"] == str(RECORD_ID)
    assert payload["outcome"] == "created"
    assert storage.calls[0]["tenant_id"] == "tenant-a"


@pytest.mark.asyncio
async def test_query_streams_strict_records(monkeypatch: pytest.MonkeyPatch) -> None:
    from contextunity.brain.service.handlers import memory as handler_module

    storage = FakeConversationStorage()
    service = MemoryServiceForTest(storage)
    monkeypatch.setattr(
        handler_module,
        "extract_token_from_context",
        lambda _context: _token(Permissions.MEMORY_READ),
    )
    unit = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "projection": "recent",
            "user_id": "user-1",
            "limit": 5,
            "offset": 0,
        }
    )
    rows = [
        ContextUnit.from_protobuf(response).payload
        async for response in service.QueryConversationHistory(
            unit.to_protobuf(contextunit_pb2), SimpleNamespace()
        )
    ]
    assert rows[0]["record_id"] == str(RECORD_ID)
    assert rows[0]["content_hash"] == "sha256:" + "a" * 64


@pytest.mark.asyncio
async def test_retention_rejects_stale_evidence_without_storage_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextunity.brain.service.handlers import memory as handler_module

    storage = FakeConversationStorage()
    service = MemoryServiceForTest(storage)
    monkeypatch.setattr(
        handler_module,
        "extract_token_from_context",
        lambda _context: ContextToken(
            token_id="retention-worker",
            permissions=(Permissions.MEMORY_WRITE,),
            allowed_tenants=("tenant-a",),
        ),
    )
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    unit = ContextUnit(
        payload={
            "tenant_id": "tenant-a",
            "record_ids": [str(RECORD_ID)],
            "cutoff": cutoff.isoformat(),
            "policy_version": "contextunity.conversation-retention/v1",
            "hold_evidence_hash": "sha256:" + "0" * 64,
        }
    )
    handler = MemoryHandlersMixin.ApplyConversationRetention.__wrapped__
    with pytest.raises(BrainValidationError, match="stale or mismatched"):
        await handler(service, unit.to_protobuf(contextunit_pb2), SimpleNamespace())
    assert storage.calls == []


def test_retention_evidence_is_exact_selection() -> None:
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    digest = conversation_retention_evidence_hash(
        tenant_id="tenant-a", cutoff=cutoff, record_ids=[RECORD_ID]
    )
    assert digest.startswith("sha256:")
