"""Shared parity tests for Brain storage backends.

Runs against SQLite by default. Postgres parity gated by
``BRAIN_TEST_DSN`` env var — skipped entirely when unset.

Run: ``uv run pytest services/brain/tests/unit/test_storage_parity.py -v``
Run against Postgres too: ``BRAIN_TEST_DSN=postgresql://... uv run pytest ...``
"""

from __future__ import annotations

import asyncio
import os
import time
from hashlib import sha256
from json import dumps as canonical_dumps
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.payloads.memory import LogTracePayload
from contextunity.brain.storage.sqlite import SqliteBrainStore

# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def sqlite_store(tmp_path: Path) -> SqliteBrainStore:
    """Fresh SQLite backend per test — isolated DB file."""
    return SqliteBrainStore(
        db_path=str(tmp_path / "test_brain.sqlite3"),
        vector_dim=8,  # small dim for fast tests
    )


@pytest.fixture
def run(sqlite_store):
    """Helper to run async methods synchronously."""

    def _run(coro):
        return asyncio.run(coro)

    return _run


BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")
TENANT = "test-tenant"
OTHER_TENANT = "other-tenant"
USER = "user-123"


def _terminal_trace(*, tenant_id: str = TENANT, graph_run_id: str | None = None) -> JsonDict:
    trace: JsonDict = {
        "schema_version": "contextunity.execution-trace/v2",
        "trace_id": str(uuid4()),
        "graph_run_id": graph_run_id or str(uuid4()),
        "tenant_id": tenant_id,
        "agent_id": "router-agent",
        "session_id": "session-a",
        "user_id": "user-a",
        "project_id": "project-a",
        "graph_name": "graph-a",
        "terminal_status": "succeeded",
        "terminal_reason": "verified_success",
        "duration_ms": 25,
        "steps": [],
        "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
        "prompt_evidence": [],
        "provenance": ["router:terminal"],
        "security_flags": [],
        "control_evidence": {
            "node_attempts": 0,
            "failed_node_attempts": 0,
            "model_attempts": 0,
            "failed_model_attempts": 0,
            "tool_attempts": 1,
            "failed_tool_attempts": 0,
            "graph_cycles": 0,
            "contribution_refs": [],
            "invalid_contribution_refs": [],
            "fault_refs": [],
            "effect_receipt_refs": ["44444444-4444-4444-8444-444444444444"],
            "effect_receipts": [
                {
                    "receipt_id": "44444444-4444-4444-8444-444444444444",
                    "operation_id": "55555555-5555-4555-8555-555555555555",
                    "idempotency_key": "66666666-6666-4666-8666-666666666666",
                    "effect_state": "committed",
                    "replay_safe": False,
                    "adapter_id": "federated:write",
                    "capability_id": "federated:write",
                    "effect_or_result_hash": "b" * 64,
                }
            ],
        },
    }
    trace["digest"] = sha256(
        canonical_dumps(trace, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return trace


def _redigest(trace: JsonDict) -> None:
    canonical = dict(trace)
    canonical.pop("digest", None)
    trace["digest"] = sha256(
        canonical_dumps(
            canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


@pytest_asyncio.fixture
async def postgres_store():
    """Live Postgres backend — skipped unless BRAIN_TEST_DSN is set.

    Must be an async fixture (not a sync one wrapping asyncio.run()): the
    underlying AsyncConnectionPool binds its background worker tasks to
    whatever event loop is running when the pool opens. A sync fixture
    handed to a test that calls asyncio.run() once per operation would
    create a new event loop on every call, orphaning the pool's workers
    from the very first call onward and hanging forever on the second.
    """
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — skipping Postgres parity test")
    from psycopg import AsyncConnection, sql

    from contextunity.brain.storage.postgres import PostgresBrainStore

    schema = f"trace_parity_{uuid4().hex}"
    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN, schema=schema)
    await store.ensure_schema()
    try:
        yield store
    finally:
        await store.close()
        admin = await AsyncConnection.connect(BRAIN_TEST_DSN, autocommit=True)
        try:
            _ = await admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
        finally:
            await admin.close()


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

    def test_expired_ref_is_distinguished_from_never_existed(self, sqlite_store, run, caplog):
        """The expired-ref count must reflect existing-but-expired ids only,
        not ids that never existed — distinct metrics for observability."""
        expired = run(
            sqlite_store.write_blackboard(
                tenant_id=TENANT,
                scope_path="expiring",
                content={},
                ttl_seconds=1,
            )
        )
        alive = run(
            sqlite_store.write_blackboard(
                tenant_id=TENANT,
                scope_path="alive",
                content={},
            )
        )
        time.sleep(1.1)

        import logging

        with caplog.at_level(logging.DEBUG, logger="contextunity.brain.storage.sqlite.blackboard"):
            records = run(
                sqlite_store.read_blackboard(
                    ids=[expired["id"], alive["id"], "00000000-0000-0000-0000-000000000000"],
                    tenant_id=TENANT,
                )
            )
        assert len(records) == 1
        assert records[0]["id"] == alive["id"]
        assert any("expired=1" in r.message for r in caplog.records)

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


class TestBlackboardPostgresParity:
    """Postgres-specific regression: id/created_at come back from psycopg3 as
    native UUID/datetime objects, not str. A prior bug used ``as_str()``
    (which only accepts already-string values) on these fields, silently
    returning "" for both on every real Postgres read — masked by every
    other test in this class running against SQLite, where these columns
    are stored as TEXT and never hit the same code path.
    """

    @pytest.mark.asyncio
    async def test_read_returns_real_id_and_created_at(self, postgres_store):
        written = await postgres_store.write_blackboard(
            tenant_id=TENANT,
            scope_path="pg.regression",
            content={"k": "v"},
        )
        records = await postgres_store.read_blackboard(ids=[written["id"]], tenant_id=TENANT)
        assert len(records) == 1
        assert records[0]["id"] == written["id"]
        assert records[0]["id"] != ""
        assert records[0]["created_at"] != ""

    @pytest.mark.asyncio
    async def test_ttl_expiry_and_expired_count(self, postgres_store):
        alive = await postgres_store.write_blackboard(
            tenant_id=TENANT, scope_path="pg.alive", content={}
        )
        expiring = await postgres_store.write_blackboard(
            tenant_id=TENANT, scope_path="pg.expiring", content={}, ttl_seconds=1
        )
        await asyncio.sleep(1.2)
        records = await postgres_store.read_blackboard(
            ids=[alive["id"], expiring["id"]], tenant_id=TENANT
        )
        assert len(records) == 1
        assert records[0]["id"] == alive["id"]

        deleted = await postgres_store.prune_expired_blackboard(tenant_id=TENANT)
        assert deleted >= 1

    @pytest.mark.asyncio
    async def test_batch_read_ten_ids(self, postgres_store):
        ids = []
        for i in range(10):
            written = await postgres_store.write_blackboard(
                tenant_id=TENANT, scope_path=f"pg.batch{i}", content={"i": i}
            )
            ids.append(written["id"])
        records = await postgres_store.read_blackboard(ids=ids, tenant_id=TENANT)
        assert len(records) == 10


@pytest.mark.skipif(
    not BRAIN_TEST_DSN, reason="BRAIN_TEST_DSN not set — skipping Postgres RLS test"
)
class TestUserWildcardRlsPostgres:
    """Live proof for the ``app.current_user = '*'`` wildcard in user-level
    RLS policies ('*' must be an admin/maintenance affordance, not blanket
    visibility). ``test_rls_policies.py`` only asserts the SQL
    text contains the wildcard — this proves the actual Postgres behavior:
    a normal user session cannot read another user's ``cells`` rows, while
    the admin wildcard session (what ``PostgresAdminOps`` uses via
    ``tenant_connection('*', user_id='*')``) sees both.
    """

    @pytest.mark.asyncio
    async def test_user_isolation_enforced_and_wildcard_bypasses_it(self, postgres_store):
        import uuid as _uuid

        from contextunity.brain.storage.postgres.models import GraphNode

        tenant = f"rls-user-{_uuid.uuid4().hex}"
        await postgres_store.upsert_graph(
            [GraphNode(id=f"{tenant}-a", content="user a cell", cell_kind="concept")],
            [],
            tenant_id=tenant,
            user_id="user-a",
        )
        await postgres_store.upsert_graph(
            [GraphNode(id=f"{tenant}-b", content="user b cell", cell_kind="concept")],
            [],
            tenant_id=tenant,
            user_id="user-b",
        )

        async def _visible_ids(user_id: str) -> set[str]:
            async with await postgres_store.tenant_connection(tenant, user_id=user_id) as conn:
                cursor = await conn.execute("SELECT id FROM cells WHERE tenant_id = %s", (tenant,))
                rows = await cursor.fetchall()
            return {row[0] for row in rows}

        assert await _visible_ids("user-a") == {f"{tenant}-a"}
        assert await _visible_ids("user-b") == {f"{tenant}-b"}
        # Admin/maintenance wildcard path — sees every user's rows.
        assert await _visible_ids("*") == {f"{tenant}-a", f"{tenant}-b"}


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

    def test_terminal_finalization_receipt_is_durable_and_idempotent(self, sqlite_store, run):
        terminal = _terminal_trace()
        created = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))
        duplicate = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))

        assert created == {
            "trace_id": terminal["trace_id"],
            "graph_run_id": terminal["graph_run_id"],
            "digest": terminal["digest"],
            "outcome": "created",
        }
        assert duplicate == {**created, "outcome": "duplicate"}
        rows = run(sqlite_store.get_traces(tenant_id=TENANT))
        assert len(rows) == 1
        assert rows[0]["id"] == terminal["trace_id"]
        assert rows[0]["trace_schema_version"] == "contextunity.execution-trace/v2"
        assert rows[0]["control_evidence"] == terminal["control_evidence"]

    def test_terminal_plan_lineage_is_preserved_in_durable_trace_metadata(self, sqlite_store, run):
        terminal = _terminal_trace()
        terminal.update(
            {
                "plan_id": "plan-2",
                "plan_revision": 2,
                "parent_plan_id": "plan-1",
                "parent_plan_revision": 1,
                "replan_ref": "44444444-4444-4444-8444-444444444444",
            }
        )
        _redigest(terminal)

        _ = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))

        row = run(sqlite_store.get_traces(tenant_id=TENANT))[0]
        assert row["metadata"] == {
            "project_id": "project-a",
            "registration_hash": None,
            "plan_id": "plan-2",
            "plan_revision": 2,
            "parent_plan_id": "plan-1",
            "parent_plan_revision": 1,
            "replan_ref": "44444444-4444-4444-8444-444444444444",
        }

    def test_payload_validation_preserves_router_digest_input(self, sqlite_store, run):
        terminal = _terminal_trace()
        validated = LogTracePayload.model_validate({"terminal_trace": terminal})
        assert validated.terminal_trace is not None
        terminal_wire = validated.terminal_trace.model_dump(mode="json", exclude_unset=True)

        receipt = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal_wire))

        assert receipt["digest"] == terminal["digest"]
        assert "registration_hash" not in terminal_wire

    def test_terminal_finalization_rejects_conflicting_digest_without_overwrite(
        self, sqlite_store, run
    ):
        terminal = _terminal_trace()
        _ = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))
        conflicting = dict(terminal)
        conflicting["agent_id"] = "different-agent"
        _redigest(conflicting)

        with pytest.raises(BrainValidationError, match="conflicting terminal trace"):
            run(sqlite_store.finalize_execution_trace(terminal_trace=conflicting))
        rows = run(sqlite_store.get_traces(tenant_id=TENANT))
        assert rows[0]["agent_id"] == "router-agent"

    def test_terminal_trace_is_tenant_scoped(self, sqlite_store, run):
        terminal = _terminal_trace(tenant_id=TENANT)
        _ = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))
        assert run(sqlite_store.get_traces(tenant_id=OTHER_TENANT)) == []
        assert run(sqlite_store.get_traces(tenant_id=TENANT, user_id="user-b")) == []
        assert len(run(sqlite_store.get_traces(tenant_id=TENANT, user_id="user-a"))) == 1
        assert len(run(sqlite_store.get_traces(tenant_id=TENANT, session_id="session-a"))) == 1

    def test_execution_trace_retention_is_age_and_tenant_scoped(self, sqlite_store, run):
        old = _terminal_trace(tenant_id=TENANT)
        recent = _terminal_trace(tenant_id=TENANT)
        other = _terminal_trace(tenant_id=OTHER_TENANT)
        for terminal in (old, recent, other):
            _ = run(sqlite_store.finalize_execution_trace(terminal_trace=terminal))
        with sqlite_store.get_sqlite_connection() as db:
            _ = db.execute(
                "UPDATE execution_traces SET created_at = datetime('now', '-60 days') "
                "WHERE id IN (?, ?)",
                (old["trace_id"], other["trace_id"]),
            )
            db.commit()

        assert (
            run(sqlite_store.delete_old_execution_traces(tenant_id=TENANT, older_than_days=30)) == 1
        )
        remaining = run(sqlite_store.get_traces(tenant_id=TENANT))
        assert [row["id"] for row in remaining] == [recent["trace_id"]]
        assert len(run(sqlite_store.get_traces(tenant_id=OTHER_TENANT))) == 1


@pytest.mark.skipif(
    not BRAIN_TEST_DSN,
    reason="BRAIN_TEST_DSN not set — skipping Postgres execution trace parity",
)
class TestExecutionTracePostgresParity:
    """Run the terminal receipt/conflict/tenant contract on live Postgres."""

    @pytest.mark.asyncio
    async def test_terminal_finalization_matches_sqlite_contract(self, postgres_store) -> None:
        tenant = f"trace-parity-{uuid4().hex}"
        terminal = _terminal_trace(tenant_id=tenant)

        created = await postgres_store.finalize_execution_trace(terminal_trace=terminal)
        duplicate = await postgres_store.finalize_execution_trace(terminal_trace=terminal)

        assert created == {
            "trace_id": terminal["trace_id"],
            "graph_run_id": terminal["graph_run_id"],
            "digest": terminal["digest"],
            "outcome": "created",
        }
        assert duplicate == {**created, "outcome": "duplicate"}
        rows = await postgres_store.get_traces(tenant_id=tenant)
        assert len(rows) == 1
        assert rows[0]["id"] == terminal["trace_id"]
        assert rows[0]["trace_schema_version"] == "contextunity.execution-trace/v2"
        assert rows[0]["control_evidence"] == terminal["control_evidence"]
        assert await postgres_store.get_traces(tenant_id=f"other-{tenant}") == []
        assert await postgres_store.get_traces(tenant_id=tenant, user_id="user-b") == []
        assert len(await postgres_store.get_traces(tenant_id=tenant, user_id="user-a")) == 1
        assert len(await postgres_store.get_traces(tenant_id=tenant, session_id="session-a")) == 1

        conflicting = dict(terminal)
        conflicting["agent_id"] = "different-agent"
        _redigest(conflicting)
        with pytest.raises(BrainValidationError, match="conflicting terminal trace"):
            await postgres_store.finalize_execution_trace(terminal_trace=conflicting)
        unchanged = await postgres_store.get_traces(tenant_id=tenant)
        assert unchanged[0]["agent_id"] == "router-agent"

    @pytest.mark.asyncio
    async def test_retention_preserves_recent_and_other_tenant_rows(self, postgres_store) -> None:
        tenant = f"trace-retention-{uuid4().hex}"
        other_tenant = f"trace-retention-other-{uuid4().hex}"
        old = _terminal_trace(tenant_id=tenant)
        recent = _terminal_trace(tenant_id=tenant)
        other = _terminal_trace(tenant_id=other_tenant)
        for terminal in (old, recent, other):
            _ = await postgres_store.finalize_execution_trace(terminal_trace=terminal)
        async with await postgres_store.tenant_connection("*", user_id="*") as conn:
            _ = await conn.execute(
                """
                UPDATE execution_traces SET created_at = now() - interval '60 days'
                WHERE id = ANY(%(ids)s)
                """,
                {"ids": [old["trace_id"], other["trace_id"]]},
            )

        assert (
            await postgres_store.delete_old_execution_traces(tenant_id=tenant, older_than_days=30)
            == 1
        )
        remaining = await postgres_store.get_traces(tenant_id=tenant)
        assert [row["id"] for row in remaining] == [recent["trace_id"]]
        assert len(await postgres_store.get_traces(tenant_id=other_tenant)) == 1


# ── Taxonomy ──────────────────────────────────────────────────────


# ── Graph ─────────────────────────────────────────────────────────


class TestGraphParity:
    """Graph: upsert nodes/edges → graph_search with max_hops."""

    def _make_nodes_and_edges(self):
        from contextunity.brain.storage.postgres.models import GraphEdge, GraphNode

        nodes = [
            GraphNode(id="n1", content="Node 1", cell_kind="concept"),
            GraphNode(id="n2", content="Node 2", cell_kind="concept"),
            GraphNode(id="n3", content="Node 3", cell_kind="concept"),
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


# ── Search (insert -> search through canonical `cells`) ────────────
#
# ``hybrid_search`` is the storage method behind the ``SearchCells`` RPC / SDK
# ``BrainClient.search_cells()`` — the primary way anything reads content back out
# of ``cells`` — yet had zero test coverage anywhere in the suite before this.
# ``TestGraphParity`` above only exercises structural graph traversal
# (``graph_search``), not content search.


class TestSearchParity:
    """Insert a cell via ``upsert_graph``, find it via ``hybrid_search``."""

    def test_insert_then_text_search_finds_cell(self, sqlite_store, run):
        from contextunity.brain.storage.postgres.models import GraphNode

        run(
            sqlite_store.upsert_graph(
                [
                    GraphNode(
                        id="cell-1",
                        content="The quick brown fox jumps over the lazy dog",
                        cell_kind="chunk",
                    )
                ],
                [],
                tenant_id=TENANT,
            )
        )

        results = run(
            sqlite_store.hybrid_search(
                query_text="quick brown fox",
                query_vec=[],
                tenant_id=TENANT,
            )
        )

        assert len(results) == 1
        assert results[0].node.id == "cell-1"
        assert "quick brown fox" in results[0].node.content

    def test_search_excludes_other_tenant(self, sqlite_store, run):
        from contextunity.brain.storage.postgres.models import GraphNode

        run(
            sqlite_store.upsert_graph(
                [GraphNode(id="cell-secret", content="classified findings", cell_kind="chunk")],
                [],
                tenant_id=TENANT,
            )
        )

        results = run(
            sqlite_store.hybrid_search(
                query_text="classified findings",
                query_vec=[],
                tenant_id=OTHER_TENANT,
            )
        )
        assert results == []

    def test_search_scope_filter_matches_ltree_descendants_literally(self, sqlite_store, run):
        from contextunity.brain.storage.postgres.models import GraphNode, ScopePath

        run(
            sqlite_store.upsert_graph(
                [
                    GraphNode(
                        id="cell-scope-good",
                        content="scoped parity marker",
                        cell_kind="chunk",
                        scope_path="acme.a_b.step",
                    ),
                    GraphNode(
                        id="cell-scope-bad",
                        content="scoped parity marker",
                        cell_kind="chunk",
                        scope_path="acme.axb.step",
                    ),
                ],
                [],
                tenant_id=TENANT,
            )
        )

        results = run(
            sqlite_store.hybrid_search(
                query_text="scoped parity marker",
                query_vec=[],
                tenant_id=TENANT,
                scope=ScopePath(path="acme.a_b"),
            )
        )

        assert [result.node.id for result in results] == ["cell-scope-good"]

    def test_search_enforces_source_and_private_visibility_filters(self, sqlite_store, run):
        run(
            sqlite_store.upsert_cell(
                tenant_id=TENANT,
                user_id=None,
                cell_kind="chunk",
                content="canonical visibility marker",
                source_type="documentation",
                visibility="tenant",
            )
        )
        run(
            sqlite_store.upsert_cell(
                tenant_id=TENANT,
                user_id="owner-a",
                cell_kind="chunk",
                content="canonical visibility marker",
                source_type="test",
                visibility="private",
            )
        )

        public_results = run(
            sqlite_store.hybrid_search(
                query_text="canonical visibility marker",
                query_vec=[],
                tenant_id=TENANT,
                source_types=["documentation"],
            )
        )
        private_without_owner = run(
            sqlite_store.hybrid_search(
                query_text="canonical visibility marker",
                query_vec=[],
                tenant_id=TENANT,
                source_types=["test"],
            )
        )
        private_with_owner = run(
            sqlite_store.hybrid_search(
                query_text="canonical visibility marker",
                query_vec=[],
                tenant_id=TENANT,
                user_id="owner-a",
                source_types=["test"],
            )
        )

        assert len(public_results) == 1
        assert public_results[0].node.source_type == "documentation"
        assert private_without_owner == []
        assert len(private_with_owner) == 1
        assert private_with_owner[0].node.visibility == "private"

    def test_search_applies_metadata_filter_before_limit(self, sqlite_store, run):
        for index in range(5):
            run(
                sqlite_store.upsert_cell(
                    tenant_id=TENANT,
                    cell_kind="chunk",
                    content="shared metadata ranking marker",
                    source_type="documentation",
                    metadata={"service": "other", "doc_type": "documentation"},
                )
            )
        wanted = run(
            sqlite_store.upsert_cell(
                tenant_id=TENANT,
                cell_kind="chunk",
                content="shared metadata ranking marker",
                source_type="documentation",
                metadata={"service": "contextunity.docs", "doc_type": "documentation"},
            )
        )

        results = run(
            sqlite_store.hybrid_search(
                query_text="shared metadata ranking marker",
                query_vec=[],
                tenant_id=TENANT,
                source_types=["documentation"],
                metadata_filter={"service": "contextunity.docs"},
                limit=1,
            )
        )

        assert [result.node.id for result in results] == [wanted["id"]]

    def test_search_ignores_non_chunk_nodes(self, sqlite_store, run):
        """Only ``cell_kind='chunk'`` cells are text-searchable — ``concept``
        nodes (used for graph-structure-only entries, e.g. TestGraphParity's
        fixtures) are excluded by design."""
        from contextunity.brain.storage.postgres.models import GraphNode

        run(
            sqlite_store.upsert_graph(
                [
                    GraphNode(
                        id="concept-1", content="unsearchable concept node", cell_kind="concept"
                    )
                ],
                [],
                tenant_id=TENANT,
            )
        )

        results = run(
            sqlite_store.hybrid_search(
                query_text="unsearchable concept",
                query_vec=[],
                tenant_id=TENANT,
            )
        )
        assert results == []


@pytest.mark.skipif(
    not BRAIN_TEST_DSN, reason="BRAIN_TEST_DSN not set — skipping Postgres parity test"
)
class TestSearchPostgresParity:
    """Same insert -> search proof against the live Postgres backend."""

    @pytest.mark.asyncio
    async def test_insert_then_text_search_finds_cell(self, postgres_store):
        from contextunity.brain.storage.postgres.models import GraphNode

        await postgres_store.upsert_graph(
            [
                GraphNode(
                    id=f"pg-search-{time.time_ns()}",
                    content="Postgres canonical cells search proof",
                    cell_kind="chunk",
                )
            ],
            [],
            tenant_id=TENANT,
        )

        results = await postgres_store.hybrid_search(
            query_text="canonical cells search proof",
            query_vec=[0.0] * 768,
            tenant_id=TENANT,
        )

        assert len(results) >= 1
        assert any("canonical cells search proof" in r.node.content for r in results)

    @pytest.mark.asyncio
    async def test_source_and_private_visibility_filters_match_sqlite(self, postgres_store):
        marker = f"postgres visibility {time.time_ns()}"
        await postgres_store.upsert_cell(
            tenant_id=TENANT,
            user_id=None,
            cell_kind="chunk",
            content=marker,
            source_type="documentation",
            visibility="tenant",
        )
        await postgres_store.upsert_cell(
            tenant_id=TENANT,
            user_id="owner-a",
            cell_kind="chunk",
            content=marker,
            source_type="test",
            visibility="private",
        )

        public_results = await postgres_store.hybrid_search(
            query_text=marker,
            query_vec=[0.0] * 768,
            tenant_id=TENANT,
            source_types=["documentation"],
        )
        private_without_owner = await postgres_store.hybrid_search(
            query_text=marker,
            query_vec=[0.0] * 768,
            tenant_id=TENANT,
            source_types=["test"],
        )
        private_with_owner = await postgres_store.hybrid_search(
            query_text=marker,
            query_vec=[0.0] * 768,
            tenant_id=TENANT,
            user_id="owner-a",
            source_types=["test"],
        )

        assert any(result.node.source_type == "documentation" for result in public_results)
        assert private_without_owner == []
        assert any(result.node.visibility == "private" for result in private_with_owner)

    @pytest.mark.asyncio
    async def test_tenant_required_for_upsert(self, postgres_store):
        """Same typed-error shape as SQLite's TestGraphParity.test_tenant_required_for_upsert
        — parity coverage must include failure paths, not just successful ones."""
        from contextunity.brain.storage.postgres.models import GraphNode

        with pytest.raises(BrainValidationError, match="tenant_id"):
            await postgres_store.upsert_graph(
                [GraphNode(id="pg-tenant-check", content="x")],
                [],
                tenant_id="",
            )


# ── Synapses# ── Synapses ─────────────────────────────────────────────────────


class TestSynapsesParity:
    """BrainSynapse: record → query → update_q, over the canonical `synapses` table."""

    def test_record_returns_defaults(self, sqlite_store, run):
        result = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
            )
        )
        assert "id" in result
        assert result["q_action"] == 0.5
        assert result["q_hypothesis"] == 0.5
        assert result["q_relevance"] == 0.5
        assert result["q_composite"] == 0.5
        assert result["status"] == "active"
        assert result["node_role"] == "worker"

    def test_q_composite_matches_formula(self, sqlite_store, run):
        from contextunity.brain.reward_constants import q_composite

        result = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
                q_action=0.9,
                q_hypothesis=0.7,
                q_relevance=0.3,
            )
        )
        expected = q_composite(0.9, 0.7, 0.3)
        assert result["q_composite"] == expected

    def test_q_values_clamped_on_record(self, sqlite_store, run):
        result = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
                q_action=1.5,
                q_hypothesis=-0.2,
            )
        )
        assert result["q_action"] == 1.0
        assert result["q_hypothesis"] == 0.0

    def test_record_round_trips_through_query(self, sqlite_store, run):
        recorded = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
                action_data={"tool": "search"},
                node_role="planner",
                q_action=0.8,
                metadata={"phase": 2},
            )
        )
        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0))
        assert len(results) == 1
        row = results[0]
        assert row["id"] == recorded["id"]
        assert row["action_data"] == {"tool": "search"}
        assert row["node_role"] == "planner"
        assert row["metadata"] == {"phase": 2}

    def test_tenant_isolation(self, sqlite_store, run):
        run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="agent-1", action_type="x"))
        results = run(sqlite_store.query_synapses(tenant_id=OTHER_TENANT, min_q=0.0))
        assert results == []

    def test_query_orders_by_q_composite_desc(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.2
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.9
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.6
            )
        )

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0))
        composites = [r["q_composite"] for r in results]
        assert composites == sorted(composites, reverse=True)

    def test_query_min_q_filters_low_scores(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.1
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.9
            )
        )

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.7))
        assert all(r["q_composite"] >= 0.7 for r in results)
        assert len(results) == 1

    def test_query_filters_by_action_type_agent_and_node_role(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="agent-a", action_type="plan", node_role="planner"
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="agent-b", action_type="tool_call", node_role="worker"
            )
        )

        results = run(
            sqlite_store.query_synapses(
                tenant_id=TENANT,
                action_type="plan",
                agent_id="agent-a",
                node_role="planner",
                min_q=0.0,
            )
        )
        assert len(results) == 1
        assert results[0]["action_type"] == "plan"

    def test_query_scope_path_matches_prefix(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", scope_path="acme.session1.step1"
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", scope_path="acme.session2"
            )
        )

        results = run(
            sqlite_store.query_synapses(tenant_id=TENANT, scope_path="acme.session1", min_q=0.0)
        )
        assert len(results) == 1
        assert results[0]["scope_path"] == "acme.session1.step1"

    def test_query_scope_path_treats_ltree_underscore_literally(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", scope_path="acme.a_b.step"
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", scope_path="acme.axb.step"
            )
        )

        results = run(
            sqlite_store.query_synapses(tenant_id=TENANT, scope_path="acme.a_b", min_q=0.0)
        )

        assert len(results) == 1
        assert results[0]["scope_path"] == "acme.a_b.step"

    def test_query_default_status_excludes_archived(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", status="active"
            )
        )
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", status="archived"
            )
        )

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0))
        assert len(results) == 1
        assert results[0]["status"] == "active"

    def test_query_explicit_status_overrides_default_set(self, sqlite_store, run):
        run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", status="archived"
            )
        )

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, status="archived", min_q=0.0))
        assert len(results) == 1
        assert results[0]["status"] == "archived"

    def test_query_respects_limit(self, sqlite_store, run):
        for _ in range(5):
            run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0, limit=2))
        assert len(results) == 2

    def test_update_q_partial_update_preserves_other_fields(self, sqlite_store, run):
        recorded = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.5, q_hypothesis=0.5
            )
        )
        updated = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id=recorded["id"],
                q_action=0.9,
            )
        )
        assert updated is not None
        assert updated["q_action"] == 0.9
        assert updated["q_hypothesis"] == 0.5  # untouched

    def test_update_q_recomputes_composite(self, sqlite_store, run):
        from contextunity.brain.reward_constants import q_composite

        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))
        updated = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id=recorded["id"],
                q_action=0.9,
                q_hypothesis=0.8,
                q_relevance=0.7,
            )
        )
        assert updated is not None
        assert updated["q_composite"] == q_composite(0.9, 0.8, 0.7)

    def test_update_q_clamps_new_values(self, sqlite_store, run):
        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))
        updated = run(
            sqlite_store.update_synapse_q(tenant_id=TENANT, synapse_id=recorded["id"], q_action=5.0)
        )
        assert updated is not None
        assert updated["q_action"] == 1.0

    def test_update_q_wrong_tenant_returns_none(self, sqlite_store, run):
        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))
        result = run(
            sqlite_store.update_synapse_q(
                tenant_id=OTHER_TENANT, synapse_id=recorded["id"], q_action=0.9
            )
        )
        assert result is None

    def test_update_q_unknown_id_returns_none(self, sqlite_store, run):
        result = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT, synapse_id="00000000-0000-0000-0000-000000000000"
            )
        )
        assert result is None

    def test_update_q_merges_metadata_patch(self, sqlite_store, run):
        recorded = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", metadata={"existing": "keep"}
            )
        )
        _ = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id=recorded["id"],
                metadata_patch={"reviewed_by": "admin"},
            )
        )
        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0))
        assert results[0]["metadata"] == {"existing": "keep", "reviewed_by": "admin"}

    def test_update_q_fault_class_and_status(self, sqlite_store, run):
        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))
        _ = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id=recorded["id"],
                fault_class="agent_fault",
                status="contradicted",
            )
        )
        results = run(
            sqlite_store.query_synapses(tenant_id=TENANT, status="contradicted", min_q=0.0)
        )
        assert len(results) == 1
        assert results[0]["fault_class"] == "agent_fault"

    def test_reward_policy_composes_with_update_synapse_q(self, sqlite_store, run):
        """End-to-end proof that reward_policy's pure functions and
        update_synapse_q's storage contract actually compose — the intended
        usage pattern for a future Router-side caller, exercised now even
        though no live caller exists yet. Also proves that update provenance
        (who/why a Q-value changed) can be recorded in metadata."""
        from contextunity.brain.reward_policy import (
            apply_node_execution_reward,
            is_trainable_tenant,
        )

        assert is_trainable_tenant(TENANT)  # ordinary tenant — safe to train from

        recorded = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="agent-1", action_type="tool_call", node_role="worker"
            )
        )
        current_q = {
            "q_action": recorded["q_action"],
            "q_hypothesis": recorded["q_hypothesis"],
            "q_relevance": recorded["q_relevance"],
        }

        new_q = apply_node_execution_reward(node_role="worker", current_q=current_q, success=True)
        assert new_q == {"q_action": pytest.approx(0.505)}

        updated = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id=recorded["id"],
                q_action=new_q["q_action"],
                metadata_patch={"reward_source": "node_execution", "reward_success": True},
                idempotency_key="node-exec-run-1",
            )
        )
        assert updated is not None
        assert updated["q_action"] == pytest.approx(0.505)

        results = run(sqlite_store.query_synapses(tenant_id=TENANT, min_q=0.0))
        assert results[0]["metadata"]["reward_source"] == "node_execution"
        assert results[0]["metadata"]["reward_success"] is True

    def test_decay_disabled_by_default(self, sqlite_store, run):
        from contextunity.brain.core.exceptions import SynapseDecayDisabledError

        with pytest.raises(SynapseDecayDisabledError):
            run(sqlite_store.decay_synapses(tenant_id=TENANT))

    def test_update_q_idempotency_key_applies_once_on_replay(self, sqlite_store, run):
        """Same update twice with the same event id applies once — replaying
        must not double-apply the Q change."""
        recorded = run(
            sqlite_store.record_synapse(
                tenant_id=TENANT, agent_id="a", action_type="x", q_action=0.5
            )
        )

        first = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.9, idempotency_key="event-1"
            )
        )
        assert first is not None
        assert first["q_action"] == 0.9

        replay = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.1, idempotency_key="event-1"
            )
        )
        assert replay is not None
        assert replay["q_action"] == 0.9  # unchanged — the 0.1 in the replay never applied

    def test_update_q_different_idempotency_keys_both_apply(self, sqlite_store, run):
        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))

        run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.6, idempotency_key="event-1"
            )
        )
        second = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.7, idempotency_key="event-2"
            )
        )
        assert second is not None
        assert second["q_action"] == 0.7

    def test_update_q_without_idempotency_key_always_applies(self, sqlite_store, run):
        recorded = run(sqlite_store.record_synapse(tenant_id=TENANT, agent_id="a", action_type="x"))

        run(
            sqlite_store.update_synapse_q(tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.6)
        )
        second = run(
            sqlite_store.update_synapse_q(tenant_id=TENANT, synapse_id=recorded["id"], q_action=0.7)
        )
        assert second is not None
        assert second["q_action"] == 0.7

    def test_update_q_unknown_id_with_idempotency_key_returns_none(self, sqlite_store, run):
        result = run(
            sqlite_store.update_synapse_q(
                tenant_id=TENANT,
                synapse_id="00000000-0000-0000-0000-000000000000",
                q_action=0.9,
                idempotency_key="event-1",
            )
        )
        assert result is None


class TestSynapsesPostgresParity:
    """Same Synapse contract against live Postgres (BRAIN_TEST_DSN)."""

    @pytest.mark.asyncio
    async def test_record_query_update_round_trip(self, postgres_store):
        import uuid

        tenant = f"synapse-parity-{uuid.uuid4().hex[:8]}"
        recorded = await postgres_store.record_synapse(
            tenant_id=tenant,
            agent_id="agent-1",
            action_type="tool_call",
            q_action=0.8,
        )
        assert recorded["q_composite"] is not None

        results = await postgres_store.query_synapses(tenant_id=tenant, min_q=0.0)
        assert len(results) == 1
        assert results[0]["id"] == recorded["id"]

        updated = await postgres_store.update_synapse_q(
            tenant_id=tenant, synapse_id=recorded["id"], q_action=0.95
        )
        assert updated is not None
        assert updated["q_action"] == 0.95

    @pytest.mark.asyncio
    async def test_tenant_isolation(self, postgres_store):
        import uuid

        tenant_a = f"synapse-a-{uuid.uuid4().hex[:8]}"
        tenant_b = f"synapse-b-{uuid.uuid4().hex[:8]}"
        await postgres_store.record_synapse(tenant_id=tenant_a, agent_id="a", action_type="x")

        results = await postgres_store.query_synapses(tenant_id=tenant_b, min_q=0.0)
        assert results == []

    @pytest.mark.asyncio
    async def test_update_q_wrong_tenant_returns_none(self, postgres_store):
        """Same None-not-raise shape as SQLite's test_update_q_wrong_tenant_returns_none."""
        import uuid

        tenant = f"synapse-wrongtenant-{uuid.uuid4().hex[:8]}"
        other_tenant = f"synapse-wrongtenant-other-{uuid.uuid4().hex[:8]}"
        recorded = await postgres_store.record_synapse(
            tenant_id=tenant, agent_id="a", action_type="x"
        )

        result = await postgres_store.update_synapse_q(
            tenant_id=other_tenant, synapse_id=recorded["id"], q_action=0.9
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_update_q_unknown_id_returns_none(self, postgres_store):
        """Same None-not-raise shape as SQLite's test_update_q_unknown_id_returns_none."""
        import uuid

        result = await postgres_store.update_synapse_q(
            tenant_id=f"synapse-unknown-{uuid.uuid4().hex[:8]}",
            synapse_id="00000000-0000-0000-0000-000000000000",
            q_action=0.9,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_generated_q_composite_matches_python_formula(self, postgres_store):
        import uuid

        from contextunity.brain.reward_constants import q_composite

        tenant = f"synapse-formula-{uuid.uuid4().hex[:8]}"
        recorded = await postgres_store.record_synapse(
            tenant_id=tenant,
            agent_id="a",
            action_type="x",
            q_action=0.8,
            q_hypothesis=0.6,
            q_relevance=0.4,
        )
        assert recorded["q_composite"] == q_composite(0.8, 0.6, 0.4)

    @pytest.mark.asyncio
    async def test_update_q_idempotency_key_applies_once_on_replay(self, postgres_store):
        """Same replay-safety proof against the real atomic-SQL idempotency
        path (not just the SQLite read-then-write path)."""
        import uuid

        tenant = f"synapse-idempotent-{uuid.uuid4().hex[:8]}"
        recorded = await postgres_store.record_synapse(
            tenant_id=tenant, agent_id="a", action_type="x", q_action=0.5
        )

        first = await postgres_store.update_synapse_q(
            tenant_id=tenant, synapse_id=recorded["id"], q_action=0.9, idempotency_key="event-1"
        )
        assert first is not None
        assert first["q_action"] == 0.9

        replay = await postgres_store.update_synapse_q(
            tenant_id=tenant, synapse_id=recorded["id"], q_action=0.1, idempotency_key="event-1"
        )
        assert replay is not None
        assert replay["q_action"] == 0.9  # unchanged — the 0.1 in the replay never applied

    @pytest.mark.asyncio
    async def test_update_q_unknown_id_with_idempotency_key_returns_none(self, postgres_store):
        result = await postgres_store.update_synapse_q(
            tenant_id="synapse-idempotent-missing",
            synapse_id="00000000-0000-0000-0000-000000000000",
            q_action=0.9,
            idempotency_key="event-1",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_reward_policy_composes_with_update_synapse_q(self, postgres_store):
        """Same end-to-end reward_policy + update_synapse_q composition proof
        as SQLite's test_reward_policy_composes_with_update_synapse_q, against
        the real atomic-SQL update path."""
        import uuid

        from contextunity.brain.reward_policy import (
            apply_node_execution_reward,
            is_trainable_tenant,
        )

        tenant = f"synapse-reward-{uuid.uuid4().hex[:8]}"
        assert is_trainable_tenant(tenant)

        recorded = await postgres_store.record_synapse(
            tenant_id=tenant, agent_id="agent-1", action_type="tool_call", node_role="worker"
        )
        current_q = {
            "q_action": recorded["q_action"],
            "q_hypothesis": recorded["q_hypothesis"],
            "q_relevance": recorded["q_relevance"],
        }
        new_q = apply_node_execution_reward(node_role="worker", current_q=current_q, success=True)
        assert new_q == {"q_action": pytest.approx(0.505)}

        updated = await postgres_store.update_synapse_q(
            tenant_id=tenant,
            synapse_id=recorded["id"],
            q_action=new_q["q_action"],
            metadata_patch={"reward_source": "node_execution", "reward_success": True},
            idempotency_key="node-exec-run-1",
        )
        assert updated is not None
        assert updated["q_action"] == pytest.approx(0.505)

        results = await postgres_store.query_synapses(tenant_id=tenant, min_q=0.0)
        assert results[0]["metadata"]["reward_source"] == "node_execution"
        assert results[0]["metadata"]["reward_success"] is True


# ── Ensure Schema ────────────────────────────────────────────────


class TestEnsureSchema:
    """Schema init must be idempotent."""

    def test_double_init(self, tmp_path):
        db_path = str(tmp_path / "double.sqlite3")
        s1 = SqliteBrainStore(db_path=db_path, vector_dim=8)
        s2 = SqliteBrainStore(db_path=db_path, vector_dim=8)
        # No crash = success
        assert s1.db_path == s2.db_path
