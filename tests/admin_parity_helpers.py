"""Shared helpers for AdminQueryProtocol parity tests (SQLite + optional Postgres)."""

from __future__ import annotations

from typing import Protocol

from contextunity.core.types import JsonDict

SYSTEM_ANALYTICS_KEYS = frozenset(
    {
        "total_traces",
        "avg_timing_ms",
        "unique_tenants",
        "unique_sessions",
        "unique_users",
        "total_input_tokens",
        "total_output_tokens",
    }
)

ANALYTICS_SUMMARY_KEYS = frozenset(
    {
        "total_traces",
        "traces_24h",
        "traces_1h",
        "avg_timing_ms",
        "p95_timing_ms",
        "total_input_tokens",
        "total_output_tokens",
        "total_tokens",
        "tokens_24h_in",
        "tokens_24h_out",
        "tokens_24h_total",
        "unique_sessions",
        "unique_users",
        "tool_usage",
        "traces_per_hour",
        "security_event_count",
        "total_cost",
        "cost_24h_total",
    }
)


class AdminOpsLike(Protocol):
    async def list_tenants(self) -> list[JsonDict]: ...
    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]: ...
    async def get_trace_details(self, trace_id: str) -> JsonDict | None: ...
    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict: ...
    async def get_session_traces(
        self, *, session_id: str, tenant_id: str | None
    ) -> list[JsonDict]: ...
    async def get_related_episodes(self, trace_id: str) -> list[JsonDict]: ...
    async def get_trace_tenant(self, trace_id: str) -> str | None: ...
    async def search_episodes(
        self,
        *,
        tenant_id: str | None,
        user_id: str | None,
        session_id: str | None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]: ...
    async def get_knowledge_nodes(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]: ...
    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict: ...
    async def get_analytics_summary(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...
    async def get_system_analytics(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...


async def assert_admin_ops_over_seeded_trace(
    ops: AdminOpsLike,
    *,
    trace_id: str,
    tenant_id: str,
    agent_id: str = "agent-a",
    session_id: str = "sess-1",
) -> None:
    """Exercise every AdminQueryProtocol method against one seeded trace."""
    tenants = await ops.list_tenants()
    assert any(str(row.get("id") or "") == tenant_id for row in tenants)

    traces, total = await ops.search_traces(
        tenant_id=None,
        agent_id=None,
        hours=None,
        limit=10,
        offset=0,
    )
    assert total >= 1
    assert any(str(row.get("id") or "") == trace_id for row in traces)

    details = await ops.get_trace_details(trace_id)
    assert details is not None
    assert details.get("agent_id") == agent_id
    assert details.get("tenant_id") == tenant_id

    system = await ops.get_system_analytics(tenant_id=None, hours=None)
    assert set(system.keys()) == SYSTEM_ANALYTICS_KEYS
    assert system["total_traces"] >= 1
    assert system["total_input_tokens"] >= 10

    summary = await ops.get_analytics_summary(tenant_id=None, hours=None)
    assert set(summary.keys()) == ANALYTICS_SUMMARY_KEYS
    assert summary["total_traces"] >= 1

    layers = await ops.get_memory_layer_stats(tenant_id=None)
    assert "episodes" in layers
    assert "knowledge_nodes" in layers

    filters = await ops.get_filter_options(tenant_id=None)
    assert agent_id in filters.get("agent_ids", [])

    session_traces = await ops.get_session_traces(session_id=session_id, tenant_id=None)
    assert len(session_traces) >= 1

    trace_tenant = await ops.get_trace_tenant(trace_id)
    assert trace_tenant == tenant_id

    episodes, episode_total = await ops.search_episodes(
        tenant_id=None,
        user_id=None,
        session_id=None,
        hours=None,
        limit=10,
        offset=0,
    )
    assert episode_total >= 0
    assert isinstance(episodes, list)

    nodes = await ops.get_knowledge_nodes(tenant_id=None, kind=None, limit=10)
    assert isinstance(nodes, list)

    related = await ops.get_related_episodes(trace_id)
    assert isinstance(related, list)


__all__ = [
    "ANALYTICS_SUMMARY_KEYS",
    "SYSTEM_ANALYTICS_KEYS",
    "AdminOpsLike",
    "assert_admin_ops_over_seeded_trace",
]
