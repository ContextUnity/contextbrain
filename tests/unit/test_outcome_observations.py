from __future__ import annotations

import sqlite3
from datetime import timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.payloads.outcomes import OutcomeObservationPayload
from contextunity.brain.storage.sqlite.store import SqliteBrainStore


def _payload(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "trace_id": str(uuid4()),
        "graph_run_id": str(uuid4()),
        "verdict_digest": "a" * 64,
        "observation_kind": "verified_success",
        "source_authority": "operator_review/v1",
        "source_ref": "review:change-123",
        "occurred_at": "2026-07-20T00:00:00Z",
        "idempotency_key": "outcome-1",
    }
    values.update(overrides)
    return values


def test_outcome_payload_is_closed_and_carries_no_q_or_synapse_authority() -> None:
    parsed = OutcomeObservationPayload.model_validate(_payload())
    assert parsed.source_authority == "operator_review/v1"
    same_instant = OutcomeObservationPayload.model_validate(
        _payload(occurred_at="2026-07-20T02:00:00+02:00")
    )
    assert parsed.occurred_at == same_instant.occurred_at
    assert same_instant.occurred_at.utcoffset() == timedelta(0)
    assert same_instant.occurred_at.tzinfo == timezone.utc

    with pytest.raises(ValidationError):
        OutcomeObservationPayload.model_validate(_payload(q_action=1.0))
    with pytest.raises(ValidationError):
        OutcomeObservationPayload.model_validate(_payload(synapse_id=str(uuid4())))
    with pytest.raises(ValidationError, match="source_authority"):
        OutcomeObservationPayload.model_validate(_payload(source_authority="model_self_report/v1"))


@pytest.mark.asyncio
async def test_sqlite_outcome_resolver_applies_attributed_synapse_once(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"))
    tenant = "tenant-a"
    run_id = str(uuid4())
    trace_id = str(uuid4())
    synapse = await store.record_synapse(
        tenant_id=tenant,
        agent_id="agent-a",
        action_type="node_execution",
        graph_name="project:p:g",
        graph_run_id=run_id,
        node_id="worker",
        node_name="worker",
        node_role="worker",
        q_action=0.5,
        q_hypothesis=0.5,
        q_relevance=0.5,
    )
    verdict_digest = "b" * 64
    with store._get_connection() as db:
        db.execute(
            """
            INSERT INTO execution_traces
                (id, tenant_id, agent_id, graph_name, graph_run_id, payload_digest,
                 terminal_status, terminal_reason, trace_schema_version, final_verdict)
            VALUES (?, ?, ?, ?, ?, ?, 'succeeded', 'verified_success',
                    'contextunity.execution-trace/v6', ?)
            """,
            (
                trace_id,
                tenant,
                "agent-a",
                "project:p:g",
                run_id,
                "c" * 64,
                '{"verdict_digest":"%s","attribution_candidates":["%s"],"fault_class":null}'
                % (verdict_digest, synapse["id"]),
            ),
        )
        db.commit()

    observation = OutcomeObservationPayload.model_validate(
        _payload(trace_id=trace_id, graph_run_id=run_id, verdict_digest=verdict_digest)
    )
    first = await store.resolve_outcome_observation(
        tenant_id=tenant,
        observation=observation,
        policy_version="contextunity.outcome-resolution/v1",
    )
    replay = await store.resolve_outcome_observation(
        tenant_id=tenant,
        observation=OutcomeObservationPayload.model_validate(
            _payload(
                trace_id=trace_id,
                graph_run_id=run_id,
                verdict_digest=verdict_digest,
                occurred_at="2026-07-20T02:00:00+02:00",
            )
        ),
        policy_version="contextunity.outcome-resolution/v1",
    )

    assert first["decision"] == "applied"
    assert first["applied_synapse_ids"] == [synapse["id"]]
    assert replay["decision"] == "duplicate"
    rows = await store.query_synapses(tenant_id=tenant, min_q=0.0)
    assert rows[0]["q_action"] > 0.5
    assert rows[0]["metadata"].get("processed_reward_events") in (None, [])
    with store._get_connection() as db:
        assert db.execute("SELECT COUNT(*) FROM outcome_synapse_effects").fetchone()[0] == 1

    conflicting = observation.model_copy(update={"observation_kind": "verified_failure"})
    with pytest.raises(BrainValidationError, match="conflicting outcome observation"):
        await store.resolve_outcome_observation(
            tenant_id=tenant,
            observation=conflicting,
            policy_version="contextunity.outcome-resolution/v1",
        )

    infra_trace = str(uuid4())
    infra_run = str(uuid4())
    infra_synapse = await store.record_synapse(
        tenant_id=tenant,
        agent_id="agent-infra",
        action_type="node_execution",
        graph_name="project:p:g",
        graph_run_id=infra_run,
        node_id="worker-infra",
        node_name="worker-infra",
        node_role="worker",
        q_action=0.5,
        q_hypothesis=0.5,
        q_relevance=0.5,
    )
    infra_digest = "d" * 64
    with store._get_connection() as db:
        db.execute(
            """INSERT INTO execution_traces
            (id, tenant_id, agent_id, graph_name, graph_run_id, payload_digest,
             terminal_status, terminal_reason, trace_schema_version, final_verdict)
            VALUES (?, ?, ?, ?, ?, ?, 'failed', 'failed',
                    'contextunity.execution-trace/v6', ?)""",
            (
                infra_trace,
                tenant,
                "agent-a",
                "project:p:g",
                infra_run,
                "e" * 64,
                '{"verdict_digest":"%s","attribution_candidates":["%s"],"fault_class":"infra_fault"}'
                % (infra_digest, infra_synapse["id"]),
            ),
        )
        db.commit()
    before_rows = await store.query_synapses(tenant_id=tenant, min_q=0.0, limit=10)
    before = next(row["q_action"] for row in before_rows if row["id"] == infra_synapse["id"])
    no_change = await store.resolve_outcome_observation(
        tenant_id=tenant,
        observation=OutcomeObservationPayload.model_validate(
            _payload(
                trace_id=infra_trace,
                graph_run_id=infra_run,
                verdict_digest=infra_digest,
                observation_kind="verified_failure",
                source_ref="review:infra",
                idempotency_key="outcome-infra",
            )
        ),
        policy_version="contextunity.outcome-resolution/v1",
    )
    after_rows = await store.query_synapses(tenant_id=tenant, min_q=0.0, limit=10)
    after = next(row["q_action"] for row in after_rows if row["id"] == infra_synapse["id"])
    assert no_change["decision"] == "no_change"
    assert after == before

    with store._get_connection() as db:
        db.execute(
            "UPDATE execution_traces SET final_verdict=? WHERE id=?",
            (
                '{"verdict_digest":"%s","attribution_candidates":["%s"],"fault_class":"agent_fault"}'
                % (infra_digest, infra_synapse["id"]),
                infra_trace,
            ),
        )
        db.commit()
    neutral = await store.resolve_outcome_observation(
        tenant_id=tenant,
        observation=OutcomeObservationPayload.model_validate(
            _payload(
                trace_id=infra_trace,
                graph_run_id=infra_run,
                verdict_digest=infra_digest,
                observation_kind="neutral",
                source_ref="review:neutral",
                idempotency_key="outcome-neutral",
            )
        ),
        policy_version="contextunity.outcome-resolution/v1",
    )
    neutral_rows = await store.query_synapses(tenant_id=tenant, min_q=0.0, limit=10)
    neutral_q = next(row["q_action"] for row in neutral_rows if row["id"] == infra_synapse["id"])
    assert neutral["decision"] == "no_change"
    assert neutral_q == before


@pytest.mark.asyncio
async def test_outcome_effect_requires_same_tenant_observation_and_synapse(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=str(tmp_path / "brain-effect-integrity.sqlite3"))
    tenant_a = "tenant-a"
    tenant_b = "tenant-b"
    synapse_a = await store.record_synapse(
        tenant_id=tenant_a,
        agent_id="agent-a",
        action_type="node_execution",
        graph_name="project:p:g",
        graph_run_id=str(uuid4()),
        node_id="node-a",
        node_name="node-a",
    )
    synapse_b = await store.record_synapse(
        tenant_id=tenant_b,
        agent_id="agent-b",
        action_type="node_execution",
        graph_name="project:p:g",
        graph_run_id=str(uuid4()),
        node_id="node-b",
        node_name="node-b",
    )
    with store._get_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """INSERT INTO outcome_synapse_effects
                (effect_id, tenant_id, observation_id, synapse_id, source_authority,
                 idempotency_key, policy_version)
                VALUES (?, ?, ?, ?, 'operator_review/v1', 'orphan',
                        'contextunity.outcome-resolution/v1')""",
                (str(uuid4()), tenant_a, str(uuid4()), str(synapse_a["id"])),
            )
            db.commit()
        db.rollback()

    foreign_trace_id = str(uuid4())
    with store._get_connection() as db:
        db.execute(
            """INSERT INTO execution_traces
            (id, tenant_id, agent_id, graph_name, graph_run_id, payload_digest,
             terminal_status, terminal_reason, trace_schema_version, final_verdict)
            VALUES (?, ?, 'agent-b', 'project:p:g', ?, ?, 'succeeded',
                    'verified_success', 'contextunity.execution-trace/v6', '{}')""",
            (foreign_trace_id, tenant_b, str(uuid4()), "d" * 64),
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """INSERT INTO outcome_observations
                (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
                 observation_kind, source_authority, source_ref, occurred_at,
                 idempotency_key, canonical_digest, policy_version, resolution_receipt)
                VALUES (?, ?, ?, ?, ?, 'neutral', 'operator_review/v1',
                        'review:cross-trace', '2026-07-20T00:00:00+00:00',
                        'cross-trace', ?, 'contextunity.outcome-resolution/v1', '{}')""",
                (
                    str(uuid4()),
                    tenant_a,
                    foreign_trace_id,
                    str(uuid4()),
                    "e" * 64,
                    "f" * 64,
                ),
            )

    trace_id = str(uuid4())
    observation_id = str(uuid4())
    with store._get_connection() as db:
        db.execute(
            """INSERT INTO execution_traces
            (id, tenant_id, agent_id, graph_name, graph_run_id, payload_digest,
             terminal_status, terminal_reason, trace_schema_version, final_verdict)
            VALUES (?, ?, 'agent-a', 'project:p:g', ?, ?, 'succeeded',
                    'verified_success', 'contextunity.execution-trace/v6', '{}')""",
            (trace_id, tenant_a, str(uuid4()), "a" * 64),
        )
        db.execute(
            """INSERT INTO outcome_observations
            (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
             observation_kind, source_authority, source_ref, occurred_at,
             idempotency_key, canonical_digest, policy_version, resolution_receipt)
            VALUES (?, ?, ?, ?, ?, 'neutral', 'operator_review/v1', 'review:scope',
                    '2026-07-20T00:00:00+00:00', 'scope', ?,
                    'contextunity.outcome-resolution/v1', '{}')""",
            (
                observation_id,
                tenant_a,
                trace_id,
                str(uuid4()),
                "b" * 64,
                "c" * 64,
            ),
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """INSERT INTO outcome_synapse_effects
                (effect_id, tenant_id, observation_id, synapse_id, source_authority,
                 idempotency_key, policy_version)
                VALUES (?, ?, ?, ?, 'operator_review/v1', 'cross-tenant',
                        'contextunity.outcome-resolution/v1')""",
                (str(uuid4()), tenant_a, observation_id, str(synapse_b["id"])),
            )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """INSERT INTO outcome_synapse_effects
                (effect_id, tenant_id, observation_id, synapse_id, source_authority,
                 idempotency_key, policy_version)
                VALUES (?, ?, ?, ?, 'unsupported/v1', 'unsupported-authority',
                        'contextunity.outcome-resolution/v1')""",
                (str(uuid4()), tenant_a, observation_id, str(synapse_a["id"])),
            )


@pytest.mark.asyncio
async def test_outcome_resolver_never_learns_from_test_tenant(tmp_path: Path) -> None:
    store = SqliteBrainStore(db_path=str(tmp_path / "brain-test.sqlite3"))
    tenant = "_test"
    run_id = str(uuid4())
    trace_id = str(uuid4())
    verdict_digest = "f" * 64
    synapse = await store.record_synapse(
        tenant_id=tenant,
        agent_id="agent-test",
        action_type="node_execution",
        graph_name="project:p:g",
        graph_run_id=run_id,
        node_id="worker-test",
        node_name="worker-test",
        node_role="worker",
        q_action=0.5,
        q_hypothesis=0.5,
        q_relevance=0.5,
    )
    with store._get_connection() as db:
        db.execute(
            """INSERT INTO execution_traces
            (id, tenant_id, agent_id, graph_name, graph_run_id, payload_digest,
             terminal_status, terminal_reason, trace_schema_version, final_verdict)
            VALUES (?, ?, ?, ?, ?, ?, 'succeeded', 'verified_success',
                    'contextunity.execution-trace/v6', ?)""",
            (
                trace_id,
                tenant,
                "agent-test",
                "project:p:g",
                run_id,
                "a" * 64,
                '{"verdict_digest":"%s","attribution_candidates":["%s"],"fault_class":null}'
                % (verdict_digest, synapse["id"]),
            ),
        )
        db.commit()

    receipt = await store.resolve_outcome_observation(
        tenant_id=tenant,
        observation=OutcomeObservationPayload.model_validate(
            _payload(
                trace_id=trace_id,
                graph_run_id=run_id,
                verdict_digest=verdict_digest,
                observation_kind="verified_success",
                source_ref="review:test-tenant",
                idempotency_key="outcome-test-tenant",
            )
        ),
        policy_version="contextunity.outcome-resolution/v1",
    )
    rows = await store.query_synapses(tenant_id=tenant, min_q=0.0)
    assert receipt["decision"] == "no_change"
    assert rows[0]["q_action"] == 0.5
