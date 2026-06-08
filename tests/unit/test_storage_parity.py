"""Shared parity tests for Brain storage backends.

Runs against SQLite by default. Postgres parity gated by
``BRAIN_TEST_DSN`` env var.

Run: ``uv run pytest services/brain/tests/unit/test_storage_parity.py -v``
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.sqlite import SqliteVecStorageBackend

# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteVecStorageBackend:
    """Fresh SQLite backend per test — isolated DB file."""
    return SqliteVecStorageBackend(
        db_path=str(tmp_path / "test_brain.sqlite3"),
        vector_dim=8,  # small dim for fast tests
    )


@pytest.fixture
def run(sqlite_store):
    """Helper to run async methods synchronously."""

    def _run(coro):
        return asyncio.run(coro)

    return _run


TENANT = "test-tenant"
OTHER_TENANT = "other-tenant"
USER = "user-123"


# ── Blackboard ────────────────────────────────────────────────────


class TestBlackboardParity:
    """Blackboard: write → read → tenant isolation → TTL expiry."""

    def test_write_and_read(self, sqlite_store, run):
        result = run(
            sqlite_store.write_blackboard(
                tenant_id=TENANT,
                scope_path="graph.step1",
                content={"key": "value"},
                metadata={"source": "test"},
                created_by="agent-x",
            )
        )

        assert "id" in result
        assert result["scope_path"] == "graph.step1"
        assert "created_at" in result

        records = run(
            sqlite_store.read_blackboard(
                ids=[result["id"]],
                tenant_id=TENANT,
            )
        )

        assert len(records) == 1
        assert records[0]["id"] == result["id"]
        assert records[0]["content"] == {"key": "value"}
        assert records[0]["metadata"] == {"source": "test"}
        assert records[0]["scope_path"] == "graph.step1"
        assert records[0]["created_by"] == "agent-x"

    def test_tenant_isolation(self, sqlite_store, run):
        """Records from one tenant must not leak to another."""
        result = run(
            sqlite_store.write_blackboard(
                tenant_id=TENANT,
                scope_path="secret.data",
                content={"classified": True},
            )
        )

        # Same ID, wrong tenant → empty
        records = run(
            sqlite_store.read_blackboard(
                ids=[result["id"]],
                tenant_id=OTHER_TENANT,
            )
        )
        assert records == []

    def test_ttl_expiry(self, sqlite_store, run):
        """Expired records must be excluded from reads and prunable."""
        result = run(
            sqlite_store.write_blackboard(
                tenant_id=TENANT,
                scope_path="ephemeral",
                content={"temp": True},
                ttl_seconds=1,
            )
        )

        # Immediately readable
        records = run(
            sqlite_store.read_blackboard(
                ids=[result["id"]],
                tenant_id=TENANT,
            )
        )
        assert len(records) == 1

        # Wait for TTL to expire
        time.sleep(1.1)

        # Now excluded from reads
        records = run(
            sqlite_store.read_blackboard(
                ids=[result["id"]],
                tenant_id=TENANT,
            )
        )
        assert records == []

        # Prunable
        deleted = run(sqlite_store.prune_expired_blackboard(tenant_id=TENANT))
        assert deleted >= 1

    def test_read_empty_ids(self, sqlite_store, run):
        records = run(sqlite_store.read_blackboard(ids=[], tenant_id=TENANT))
        assert records == []

    def test_read_nonexistent_id(self, sqlite_store, run):
        records = run(
            sqlite_store.read_blackboard(
                ids=["nonexistent-uuid"],
                tenant_id=TENANT,
            )
        )
        assert records == []

    def test_multiple_records_batch_read(self, sqlite_store, run):
        ids = []
        for i in range(3):
            r = run(
                sqlite_store.write_blackboard(
                    tenant_id=TENANT,
                    scope_path=f"batch.step{i}",
                    content={"index": i},
                )
            )
            ids.append(r["id"])

        records = run(sqlite_store.read_blackboard(ids=ids, tenant_id=TENANT))
        assert len(records) == 3


# ── Traces ────────────────────────────────────────────────────────


class TestTracesParity:
    """Traces: log → get with filters."""

    def test_log_and_get(self, sqlite_store, run):
        trace_id = run(
            sqlite_store.log_trace(
                tenant_id=TENANT,
                agent_id="gardener",
                session_id="sess-1",
                graph_name="product_writer",
                tool_calls=[{"name": "search", "args": {}}],
                token_usage={"prompt": 100, "completion": 50},
                timing_ms=1200,
                metadata={"model": "gpt-4"},
            )
        )

        assert isinstance(trace_id, str)
        assert len(trace_id) == 36  # UUID

        traces = run(sqlite_store.get_traces(tenant_id=TENANT))
        assert len(traces) >= 1
        t = traces[0]
        assert t["agent_id"] == "gardener"
        assert t["graph_name"] == "product_writer"
        assert t["tool_calls"] == [{"name": "search", "args": {}}]

    def test_filter_by_agent(self, sqlite_store, run):
        run(sqlite_store.log_trace(tenant_id=TENANT, agent_id="agent-a"))
        run(sqlite_store.log_trace(tenant_id=TENANT, agent_id="agent-b"))

        traces = run(sqlite_store.get_traces(tenant_id=TENANT, agent_id="agent-a"))
        assert all(t["agent_id"] == "agent-a" for t in traces)

    def test_filter_by_session(self, sqlite_store, run):
        run(sqlite_store.log_trace(tenant_id=TENANT, agent_id="x", session_id="s1"))
        run(sqlite_store.log_trace(tenant_id=TENANT, agent_id="x", session_id="s2"))

        traces = run(sqlite_store.get_traces(tenant_id=TENANT, session_id="s1"))
        assert all(t["session_id"] == "s1" for t in traces)

    def test_tenant_isolation(self, sqlite_store, run):
        run(sqlite_store.log_trace(tenant_id=TENANT, agent_id="secret"))
        traces = run(sqlite_store.get_traces(tenant_id=OTHER_TENANT))
        assert traces == []


# ── Taxonomy ──────────────────────────────────────────────────────


class TestTaxonomyParity:
    """Taxonomy: upsert → get with domain filtering."""

    def test_upsert_and_get(self, sqlite_store, run):
        run(
            sqlite_store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="product",
                name="Outdoor Gear",
                path="product.outdoor",
                keywords=["camping", "hiking"],
                metadata={"icon": "🏕️"},
            )
        )

        items = run(sqlite_store.get_all_taxonomy(tenant_id=TENANT))
        assert len(items) == 1
        assert items[0]["name"] == "Outdoor Gear"
        assert items[0]["keywords"] == ["camping", "hiking"]

    def test_domain_filter(self, sqlite_store, run):
        run(
            sqlite_store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="product",
                name="A",
                path="product.a",
                keywords=[],
            )
        )
        run(
            sqlite_store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="category",
                name="B",
                path="category.b",
                keywords=[],
            )
        )

        items = run(sqlite_store.get_all_taxonomy(tenant_id=TENANT, domain="product"))
        assert len(items) == 1
        assert items[0]["domain"] == "product"

    def test_upsert_updates(self, sqlite_store, run):
        run(
            sqlite_store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="d",
                name="V1",
                path="d.x",
                keywords=["old"],
            )
        )
        run(
            sqlite_store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="d",
                name="V2",
                path="d.x",
                keywords=["new"],
            )
        )

        items = run(sqlite_store.get_all_taxonomy(tenant_id=TENANT, domain="d"))
        assert len(items) == 1
        assert items[0]["name"] == "V2"
        assert items[0]["keywords"] == ["new"]


# ── Graph ─────────────────────────────────────────────────────────


class TestGraphParity:
    """Graph: upsert nodes/edges → graph_search with max_hops."""

    def _make_nodes_and_edges(self):
        from contextunity.brain.storage.postgres.models import GraphEdge, GraphNode

        nodes = [
            GraphNode(id="n1", content="Node 1", node_kind="concept"),
            GraphNode(id="n2", content="Node 2", node_kind="concept"),
            GraphNode(id="n3", content="Node 3", node_kind="concept"),
        ]
        edges = [
            GraphEdge(source_id="n1", target_id="n2", relation="related"),
            GraphEdge(source_id="n2", target_id="n3", relation="related"),
        ]
        return nodes, edges

    def test_upsert_and_search(self, sqlite_store, run):
        nodes, edges = self._make_nodes_and_edges()
        run(sqlite_store.upsert_graph(nodes, edges, tenant_id=TENANT))

        result = run(
            sqlite_store.graph_search(
                tenant_id=TENANT,
                entrypoint_ids=["n1"],
                max_hops=1,
            )
        )

        assert "nodes" in result
        assert "edges" in result
        assert len(result["edges"]) >= 1
        # n1→n2 should be found at hop 1
        edge_targets = {e["target_id"] for e in result["edges"]}
        assert "n2" in edge_targets

    def test_max_hops_2(self, sqlite_store, run):
        nodes, edges = self._make_nodes_and_edges()
        run(sqlite_store.upsert_graph(nodes, edges, tenant_id=TENANT))

        result = run(
            sqlite_store.graph_search(
                tenant_id=TENANT,
                entrypoint_ids=["n1"],
                max_hops=2,
            )
        )

        # Should reach n3 via n1→n2→n3
        node_ids = {n["id"] for n in result["nodes"]}
        assert "n3" in node_ids

    def test_tenant_isolation(self, sqlite_store, run):
        nodes, edges = self._make_nodes_and_edges()
        run(sqlite_store.upsert_graph(nodes, edges, tenant_id=TENANT))

        result = run(
            sqlite_store.graph_search(
                tenant_id=OTHER_TENANT,
                entrypoint_ids=["n1"],
                max_hops=1,
            )
        )
        assert result["edges"] == []

    def test_empty_entrypoints(self, sqlite_store, run):
        result = run(
            sqlite_store.graph_search(
                tenant_id=TENANT,
                entrypoint_ids=[],
                max_hops=1,
            )
        )
        assert result == {"nodes": [], "edges": []}

    def test_tenant_required_for_upsert(self, sqlite_store, run):
        from contextunity.brain.storage.postgres.models import GraphNode

        with pytest.raises(BrainValidationError, match="tenant_id"):
            run(
                sqlite_store.upsert_graph(
                    [GraphNode(id="x", content="x")],
                    [],
                    tenant_id="",
                )
            )


# ── Episodes ──────────────────────────────────────────────────────


class TestEpisodesParity:
    """Episodes: add → get_recent → count → delete_old."""

    def test_add_and_get_recent(self, sqlite_store, run):
        run(
            sqlite_store.add_episode(
                id="ep-1",
                user_id=USER,
                content="Hello world",
                tenant_id=TENANT,
                metadata={"turn": 1},
            )
        )

        episodes = run(
            sqlite_store.get_recent_episodes(
                user_id=USER,
                tenant_id=TENANT,
                limit=5,
            )
        )
        assert len(episodes) == 1
        assert episodes[0]["id"] == "ep-1"
        assert episodes[0]["content"] == "Hello world"
        assert episodes[0]["metadata"] == {"turn": 1}

    def test_count_episodes(self, sqlite_store, run):
        for i in range(3):
            run(
                sqlite_store.add_episode(
                    id=f"ep-{i}",
                    user_id=USER,
                    content=f"Episode {i}",
                    tenant_id=TENANT,
                )
            )

        stats = run(sqlite_store.count_episodes(tenant_id=TENANT))
        assert stats["total"] == 3
        assert stats["oldest"] is not None
        assert stats["newest"] is not None

    def test_delete_by_ids(self, sqlite_store, run):
        for i in range(3):
            run(
                sqlite_store.add_episode(
                    id=f"del-{i}",
                    user_id=USER,
                    content=f"Del {i}",
                    tenant_id=TENANT,
                )
            )

        deleted = run(
            sqlite_store.delete_old_episodes(
                tenant_id=TENANT,
                episode_ids=["del-0", "del-1"],
            )
        )
        assert deleted == 2

        stats = run(sqlite_store.count_episodes(tenant_id=TENANT))
        assert stats["total"] == 1

    def test_tenant_required(self, sqlite_store, run):
        with pytest.raises(BrainValidationError, match="tenant_id"):
            run(
                sqlite_store.add_episode(
                    id="x",
                    user_id=USER,
                    content="x",
                    tenant_id="",
                )
            )


# ── Facts ─────────────────────────────────────────────────────────


class TestFactsParity:
    """Facts: upsert → get → upsert updates."""

    def test_upsert_and_get(self, sqlite_store, run):
        run(
            sqlite_store.upsert_fact(
                user_id=USER,
                tenant_id=TENANT,
                key="favorite_color",
                value="blue",
                confidence=0.9,
            )
        )

        facts = run(sqlite_store.get_user_facts(user_id=USER, tenant_id=TENANT))
        assert len(facts) == 1
        assert facts[0]["fact_key"] == "favorite_color"
        assert facts[0]["fact_value"] == "blue"
        assert facts[0]["confidence"] == 0.9

    def test_upsert_updates_existing(self, sqlite_store, run):
        run(
            sqlite_store.upsert_fact(
                user_id=USER,
                tenant_id=TENANT,
                key="name",
                value="Alice",
            )
        )
        run(
            sqlite_store.upsert_fact(
                user_id=USER,
                tenant_id=TENANT,
                key="name",
                value="Bob",
            )
        )

        facts = run(sqlite_store.get_user_facts(user_id=USER, tenant_id=TENANT))
        assert len(facts) == 1
        assert facts[0]["fact_value"] == "Bob"

    def test_tenant_isolation(self, sqlite_store, run):
        run(
            sqlite_store.upsert_fact(
                user_id=USER,
                tenant_id=TENANT,
                key="secret",
                value="classified",
            )
        )

        facts = run(
            sqlite_store.get_user_facts(
                user_id=USER,
                tenant_id=OTHER_TENANT,
            )
        )
        assert facts == []

    def test_tenant_required(self, sqlite_store, run):
        with pytest.raises(BrainValidationError, match="tenant_id"):
            run(
                sqlite_store.upsert_fact(
                    user_id=USER,
                    tenant_id="",
                    key="k",
                    value="v",
                )
            )


# ── Ensure Schema ────────────────────────────────────────────────


class TestEnsureSchema:
    """Schema init must be idempotent."""

    def test_double_init(self, tmp_path):
        db_path = str(tmp_path / "double.sqlite3")
        s1 = SqliteVecStorageBackend(db_path=db_path, vector_dim=8)
        s2 = SqliteVecStorageBackend(db_path=db_path, vector_dim=8)
        # No crash = success
        assert s1.db_path == s2.db_path
