"""Admin RPC SQLite backend — local Brain path for Forge dashboard."""

from __future__ import annotations

from pathlib import Path

import pytest

from contextunity.brain.storage.sqlite import SqliteBrainStore
from contextunity.brain.storage.sqlite.admin_ops import SqliteAdminOps


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=tmp_path / "brain.sqlite3", vector_dim=8)


@pytest.mark.asyncio
async def test_sqlite_admin_search_traces_and_analytics(
    sqlite_store: SqliteBrainStore,
) -> None:
    trace_id = await sqlite_store.log_trace(
        tenant_id="default",
        agent_id="agent-a",
        session_id="sess-1",
        user_id="user-1",
        tool_calls=[{"tool": "search_docs", "status": "ok"}],
        token_usage={"input_tokens": 10, "output_tokens": 5, "total_cost": 0.01},
        timing_ms=120,
    )
    assert trace_id

    ops = SqliteAdminOps(sqlite_store)
    tenants = ops.list_tenants()
    assert any(t["id"] == "default" for t in tenants)

    traces, total = ops.search_traces(
        tenant_id=None,
        agent_id=None,
        hours=None,
        limit=10,
        offset=0,
    )
    assert total == 1
    assert traces[0]["id"] == trace_id

    details = ops.get_trace_details(trace_id)
    assert details is not None
    assert details["agent_id"] == "agent-a"

    layers = ops.get_memory_layer_stats(tenant_id=None)
    assert "episodic_events" in layers
    assert "cells" in layers

    analytics = ops.get_analytics_summary(tenant_id=None, hours=None)
    assert analytics["total_traces"] == 1
    assert analytics["total_input_tokens"] == 10
    assert analytics["tool_usage"].get("search_docs") == 1

    filters = ops.get_filter_options(tenant_id=None)
    assert "agent-a" in filters.get("agent_ids", [])

    session_traces = ops.get_session_traces(session_id="sess-1", tenant_id=None)
    assert len(session_traces) == 1

    events, event_total = ops.search_episodes(
        tenant_id=None,
        user_id=None,
        session_id=None,
        hours=None,
        limit=10,
        offset=0,
    )
    assert event_total == 0
    assert events == []

    system_analytics = ops.get_system_analytics(tenant_id=None, hours=None)
    assert system_analytics["total_traces"] == 1
    assert system_analytics["avg_timing_ms"] == 120
    assert system_analytics["unique_tenants"] == 1
    assert system_analytics["unique_sessions"] == 1
    assert system_analytics["unique_users"] == 1
    assert system_analytics["total_input_tokens"] == 10
    assert system_analytics["total_output_tokens"] == 5


@pytest.mark.asyncio
async def test_memory_layer_stats_include_cell_source_types(
    sqlite_store: SqliteBrainStore,
) -> None:
    for index, source_type in enumerate(
        ("auto_extract", "documentation", "documentation", "synthesis", "manual")
    ):
        await sqlite_store.upsert_cell(
            tenant_id="demo",
            cell_kind="documentation" if source_type == "documentation" else "fact",
            content=f"{source_type} cell {index}",
            source_type=source_type,
        )

    stats = SqliteAdminOps(sqlite_store).get_memory_layer_stats(tenant_id="demo")

    assert stats["cells"] == {
        "count": 5,
        "by_source_type": {
            "auto_extract": 1,
            "documentation": 2,
            "manual": 1,
            "synthesis": 1,
        },
    }
    assert stats["embedding_jobs"] == {
        "pending": 0,
        "processing": 0,
        "ready": 0,
        "failed": 0,
        "skipped": 0,
    }


@pytest.mark.asyncio
async def test_memory_layer_stats_include_embedding_job_states(
    sqlite_store: SqliteBrainStore,
) -> None:
    cell = await sqlite_store.upsert_cell(
        tenant_id="demo",
        cell_kind="documentation",
        content="queue state",
        content_hash="sha256:queue-state",
        source_type="documentation",
    )
    await sqlite_store.enqueue_embedding_job(
        tenant_id="demo",
        cell_id=str(cell["id"]),
        content_hash="sha256:queue-state",
        profile="default",
        max_pending=10,
    )

    stats = SqliteAdminOps(sqlite_store).get_memory_layer_stats(tenant_id="demo")

    assert stats["embedding_jobs"]["pending"] == 1
