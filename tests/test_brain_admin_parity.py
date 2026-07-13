"""Backend-parametrized AdminQueryProtocol parity tests.

SQLite runs in default CI. Postgres runs when ``BRAIN_TEST_DSN`` is set
(``@pytest.mark.integration_live``).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from admin_parity_helpers import assert_admin_ops_over_seeded_trace

from contextunity.brain.storage.admin_factory import create_admin_ops
from contextunity.brain.storage.contracts import AdminQueryProtocol
from contextunity.brain.storage.postgres import PostgresBrainStore
from contextunity.brain.storage.postgres.store.admin import PostgresAdminOps
from contextunity.brain.storage.sqlite import SqliteBrainStore
from contextunity.brain.storage.sqlite.admin_ops import AsyncSqliteAdminOps

BRAIN_TEST_DSN = (os.environ.get("BRAIN_TEST_DSN") or os.environ.get("POSTGRES_DSN") or "").strip()

TENANT = "demo"
AGENT = "agent-a"
SESSION = "sess-1"
USER = "user-1"


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)


@pytest.fixture
def run():
    def _run(coro):
        return asyncio.run(coro)

    return _run


async def _seed_trace(store: SqliteBrainStore | PostgresBrainStore, *, tenant: str) -> str:
    trace_id = await store.log_trace(
        tenant_id=tenant,
        agent_id=AGENT,
        session_id=SESSION,
        user_id=USER,
        tool_calls=[{"tool": "search_docs", "status": "ok"}],
        token_usage={"input_tokens": 10, "output_tokens": 5, "total_cost": 0.01},
        timing_ms=120,
    )
    assert trace_id
    return trace_id


@pytest.mark.asyncio
async def test_sqlite_admin_ops_parity_via_factory(sqlite_store: SqliteBrainStore) -> None:
    trace_id = await _seed_trace(sqlite_store, tenant=TENANT)
    ops = create_admin_ops(sqlite_store)
    assert isinstance(ops, AsyncSqliteAdminOps)
    await assert_admin_ops_over_seeded_trace(ops, trace_id=trace_id, tenant_id=TENANT)


@pytest.mark.asyncio
async def test_sqlite_source_type_stats(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=tmp_path / "stats.sqlite3", vector_dim=8)
    try:
        for index, source_type in enumerate(("auto_extract", "documentation", "synthesis")):
            await store.upsert_cell(
                tenant_id=TENANT,
                cell_kind="documentation",
                content=f"source stats {index}",
                source_type=source_type,
            )
        stats = await AsyncSqliteAdminOps(store).get_memory_layer_stats(tenant_id=TENANT)
        assert stats["cells"]["by_source_type"] == {
            "auto_extract": 1,
            "documentation": 1,
            "synthesis": 1,
        }
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_create_admin_ops_rejects_unknown_storage() -> None:
    from contextunity.brain.core.exceptions import BrainStorageError

    class _UnknownStore:
        pass

    with pytest.raises(BrainStorageError, match="not supported"):
        create_admin_ops(_UnknownStore())


@pytest.mark.integration_live
@pytest.mark.asyncio
async def test_postgres_admin_ops_parity() -> None:
    if not BRAIN_TEST_DSN:
        pytest.skip("Set BRAIN_TEST_DSN or POSTGRES_DSN for Postgres admin parity")

    tenant = f"parity-{uuid.uuid4().hex[:8]}"
    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    await store.ensure_schema()
    trace_id = await _seed_trace(store, tenant=tenant)

    ops: AdminQueryProtocol = PostgresAdminOps(store)
    await assert_admin_ops_over_seeded_trace(ops, trace_id=trace_id, tenant_id=tenant)


@pytest.mark.integration_live
@pytest.mark.asyncio
async def test_postgres_source_type_stats() -> None:
    if not BRAIN_TEST_DSN:
        pytest.skip("Set BRAIN_TEST_DSN or POSTGRES_DSN for Postgres admin parity")

    tenant = f"stats-{uuid.uuid4().hex[:8]}"
    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    await store.ensure_schema()
    for index, source_type in enumerate(("auto_extract", "documentation", "synthesis")):
        await store.upsert_cell(
            tenant_id=tenant,
            cell_kind="documentation",
            content=f"source stats {index}",
            source_type=source_type,
        )
    stats = await PostgresAdminOps(store).get_memory_layer_stats(tenant_id=tenant)
    assert stats["cells"]["by_source_type"] == {
        "auto_extract": 1,
        "documentation": 1,
        "synthesis": 1,
    }
