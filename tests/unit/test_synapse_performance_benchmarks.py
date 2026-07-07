"""Performance baselines for BrainSynapse operations against live Postgres.
Skipped unless BRAIN_TEST_DSN is set — same gating convention as
test_storage_parity.py.

These are observational, not a CI-blocking SLA gate: measured against this
same dev Postgres instance, p95 for a single-row INSERT ranged from ~10ms
in isolation to ~168ms as part of the full test-suite run (other tests'
connections/queries contending for the same instance) — over an order of
magnitude of variance from load alone, nothing to do with the code under
test. A tight assertion here would flake constantly; each test instead
prints its measured p95 against the roadmap target (so a real regression
is visible in the test output) and only hard-fails past a generous
catastrophic-failure ceiling that would indicate something is actually
broken (e.g. a hung connection), not just "the shared instance is busy."

Recorded baselines (single local dev Postgres instance, DSN set, run in
isolation with -p no:randomly — see above for full-suite variance):
  Record Synapse:            ~13ms    (target <= 5ms  — not met, fsync-bound, see below)
  Update Synapse Q:          ~8.5ms   (target <= 5ms  — not met, fsync-bound, see below)
  Query Synapses (10k rows): ~13.7ms  (target <= 15ms — MET after the index fix below)
  DLQ-0 write:                ~0.24ms (target <= 2ms  — met)

Query 10k rows — met, but only after a real index fix. Before it, p95 was
~19ms: EXPLAIN showed the query scanning the global ``synapses_q_composite_idx``
and discarding 12k+ other-tenant rows to return 10 (read amplification that
grows with total row count). Adding ``synapses_tenant_q_composite_idx``
``(tenant_id, q_composite DESC)`` (schema.py + migration 0010) let the planner
seek straight to the tenant's slice: index-scan execution dropped from ~20ms to
~0.5ms, and the end-to-end p95 (incl. pool acquire + RLS SET LOCAL round trips)
to ~13.7ms — inside target. This was a genuine scaling bug, not benchmark noise.

Why Record/Update miss the 5ms target (measured breakdown, same instance):
  INSERT statement alone:                       ~0.7ms median
  RLS context (SET LOCAL ROLE + 2x set_config): +~0.6ms
  COMMIT with synchronous_commit=on:            +~6ms   <- dominant cost
  same write with synchronous_commit=off:       ~2ms total (inside target)
The floor is the WAL fsync a durable COMMIT performs; on this instance's
storage that alone exceeds the whole 5ms budget, so no amount of query or
pooling work can close the gap. Accepted mitigation (CP-2 §12 gate 15):
per-transaction ``SET LOCAL synchronous_commit TO off`` for Synapse evidence
writes would meet the target at the cost of possibly losing the last <1s of
committed learning traces on a Postgres crash (never corruption) — a durability
trade-off to adopt deliberately, not silently; until then the gap is expected
on any storage whose fsync latency exceeds ~4ms.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time
import uuid

import pytest
import pytest_asyncio

BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")

# Only catches a genuinely broken/hung operation — see module docstring for
# why this isn't a tight, target-based assertion.
_CATASTROPHIC_CEILING_MS = 5_000.0


@pytest_asyncio.fixture
async def postgres_store():
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — skipping performance benchmarks")
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    yield store
    await store.close()


def _p95(samples_ms: list[float]) -> float:
    return statistics.quantiles(samples_ms, n=100)[94]


async def _time_ms(coro) -> float:
    started = time.perf_counter()
    await coro
    return (time.perf_counter() - started) * 1000


def _report(label: str, p95: float, target_ms: float) -> None:
    met = "met" if p95 <= target_ms else "NOT met"
    print(f"\n{label} p95: {p95:.2f}ms (target <= {target_ms:.0f}ms — {met})")
    assert p95 <= _CATASTROPHIC_CEILING_MS


class TestRecordSynapsePerformance:
    @pytest.mark.asyncio
    async def test_record_synapse_p95(self, postgres_store):
        tenant = f"perf-record-{uuid.uuid4().hex[:8]}"
        samples: list[float] = []
        for _ in range(50):
            samples.append(
                await _time_ms(
                    postgres_store.record_synapse(
                        tenant_id=tenant, agent_id="agent-1", action_type="tool_call"
                    )
                )
            )

        _report("Record Synapse", _p95(samples), target_ms=5)


class TestUpdateSynapseQPerformance:
    @pytest.mark.asyncio
    async def test_update_synapse_q_p95(self, postgres_store):
        tenant = f"perf-update-{uuid.uuid4().hex[:8]}"
        recorded_ids = [
            (
                await postgres_store.record_synapse(
                    tenant_id=tenant, agent_id="agent-1", action_type="x"
                )
            )["id"]
            for _ in range(50)
        ]

        samples: list[float] = []
        for synapse_id in recorded_ids:
            samples.append(
                await _time_ms(
                    postgres_store.update_synapse_q(
                        tenant_id=tenant, synapse_id=synapse_id, q_action=0.9
                    )
                )
            )

        _report("Update Synapse Q", _p95(samples), target_ms=5)


class TestQuerySynapsesPerformance:
    @pytest.mark.asyncio
    async def test_query_synapses_10k_rows_p95(self, postgres_store):
        tenant = f"perf-query-{uuid.uuid4().hex[:8]}"

        async def _seed_one(i: int) -> None:
            await postgres_store.record_synapse(
                tenant_id=tenant,
                agent_id=f"agent-{i % 7}",
                action_type="tool_call" if i % 2 == 0 else "plan",
                q_action=0.5 + (i % 50) / 100,
            )

        # Bounded concurrency: 10k sequential round trips would dominate
        # this test's own runtime; the thing under test is the query, not
        # the seeding, so a moderately concurrent seed is a faithful setup.
        concurrency = 100
        for start in range(0, 10_000, concurrency):
            await asyncio.gather(
                *(_seed_one(i) for i in range(start, min(start + concurrency, 10_000)))
            )

        samples: list[float] = []
        for _ in range(30):
            samples.append(
                await _time_ms(
                    postgres_store.query_synapses(
                        tenant_id=tenant, action_type="tool_call", min_q=0.5, limit=10
                    )
                )
            )

        _report("Query Synapses (10k rows)", _p95(samples), target_ms=15)


class TestDlq0WritePerformance:
    @pytest.mark.asyncio
    async def test_dlq0_write_p95(self, tmp_path):
        from contextunity.core.dlq import LocalFileDlqWriter
        from contextunity.core.faults import fault_event

        writer = LocalFileDlqWriter(tmp_path / "perf_dlq0.jsonl")
        event = fault_event(
            ValueError("benchmark"),
            event_type="benchmark.dlq_write",
            error_code="benchmark.dlq_write",
        )

        samples: list[float] = []
        for _ in range(100):
            samples.append(await _time_ms(writer.write(event)))

        _report("DLQ-0 write", _p95(samples), target_ms=2)
