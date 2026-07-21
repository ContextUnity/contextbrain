"""Transactional OutcomeObservation resolution for PostgreSQL/RLS."""

from __future__ import annotations

import hashlib
import json
from abc import ABC
from datetime import timezone
from uuid import uuid4

from contextunity.core.narrowing import as_float, str_list_as_json
from contextunity.core.types import JsonDict, is_json_dict
from psycopg.types.json import Jsonb

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.payloads.outcomes import OutcomeObservationPayload
from contextunity.brain.reward_policy import apply_node_execution_reward, is_trainable_tenant

from .base import PostgresStoreBase
from .helpers import fetch_all


def _digest(observation: OutcomeObservationPayload) -> str:
    encoded = json.dumps(
        observation.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class OutcomeObservationsMixin(PostgresStoreBase, ABC):
    async def import_outcome_observation_record(self, *, tenant_id: str, record: JsonDict) -> None:
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT canonical_digest FROM outcome_observations WHERE tenant_id=%(tenant_id)s AND observation_id=%(observation_id)s",
                {"tenant_id": tenant_id, "observation_id": str(record["observation_id"])},
            )
            if rows:
                if str(rows[0].get("canonical_digest")) != str(record["canonical_digest"]):
                    raise BrainValidationError("conflicting imported outcome observation")
                return
            await fetch_all(
                conn,
                """INSERT INTO outcome_observations
                (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
                 observation_kind, source_authority, source_ref, occurred_at, idempotency_key,
                 canonical_digest, policy_version, resolution_receipt, created_at)
                VALUES (%(observation_id)s, %(tenant_id)s, %(trace_id)s, %(graph_run_id)s,
                 %(verdict_digest)s, %(observation_kind)s, %(source_authority)s, %(source_ref)s,
                 %(occurred_at)s, %(idempotency_key)s, %(canonical_digest)s,
                 %(policy_version)s, %(resolution_receipt)s, %(created_at)s)
                RETURNING observation_id""",
                {
                    **record,
                    "tenant_id": tenant_id,
                    "resolution_receipt": Jsonb(record["resolution_receipt"]),
                },
            )

    async def resolve_outcome_observation(
        self,
        *,
        tenant_id: str,
        observation: OutcomeObservationPayload,
        policy_version: str,
    ) -> JsonDict:
        canonical_digest = _digest(observation)
        async with await self.tenant_connection(tenant_id) as conn:
            await fetch_all(
                conn,
                "SELECT pg_advisory_xact_lock(hashtextextended(%(lock_key)s, 0))",
                {
                    "lock_key": (
                        f"outcome:{tenant_id}:{observation.source_authority}:"
                        f"{observation.idempotency_key}"
                    )
                },
            )
            existing = await fetch_all(
                conn,
                """
                SELECT canonical_digest, resolution_receipt
                FROM outcome_observations
                WHERE tenant_id=%(tenant_id)s AND source_authority=%(source_authority)s
                  AND idempotency_key=%(idempotency_key)s
                FOR UPDATE
                """,
                {
                    "tenant_id": tenant_id,
                    "source_authority": observation.source_authority,
                    "idempotency_key": observation.idempotency_key,
                },
            )
            if existing:
                if str(existing[0].get("canonical_digest")) != canonical_digest:
                    raise BrainValidationError("conflicting outcome observation idempotency key")
                raw = existing[0].get("resolution_receipt")
                if not is_json_dict(raw):
                    raise BrainValidationError("stored outcome resolution receipt is malformed")
                receipt = dict(raw)
                receipt["decision"] = "duplicate"
                return receipt

            traces = await fetch_all(
                conn,
                """
                SELECT graph_run_id::text, final_verdict
                FROM execution_traces
                WHERE tenant_id=%(tenant_id)s AND id=%(trace_id)s
                FOR UPDATE
                """,
                {"tenant_id": tenant_id, "trace_id": str(observation.trace_id)},
            )
            if not traces:
                raise BrainValidationError("outcome observation terminal trace not found")
            if str(traces[0].get("graph_run_id")) != str(observation.graph_run_id):
                raise BrainValidationError("outcome observation graph run mismatch")
            verdict = traces[0].get("final_verdict")
            if (
                not is_json_dict(verdict)
                or verdict.get("verdict_digest") != observation.verdict_digest
            ):
                raise BrainValidationError("outcome observation FinalVerdict mismatch")
            candidates = verdict.get("attribution_candidates")
            if not isinstance(candidates, list):
                raise BrainValidationError("FinalVerdict attribution candidates are malformed")
            candidate_ids = [item for item in candidates if isinstance(item, str)]
            if len(candidate_ids) != len(candidates):
                raise BrainValidationError("FinalVerdict attribution candidates are malformed")

            success = observation.observation_kind == "verified_success"
            verified_failure = observation.observation_kind == "verified_failure"
            eligible = is_trainable_tenant(tenant_id) and (
                success or (verified_failure and verdict.get("fault_class") == "agent_fault")
            )
            observation_id = str(uuid4())
            applied: list[str] = []
            if eligible:
                for synapse_id in candidate_ids:
                    rows = await fetch_all(
                        conn,
                        """
                        SELECT id::text, node_role, q_action, q_hypothesis, q_relevance
                        FROM synapses
                        WHERE tenant_id=%(tenant_id)s AND id=%(synapse_id)s
                          AND graph_run_id=%(graph_run_id)s
                        FOR UPDATE
                        """,
                        {
                            "tenant_id": tenant_id,
                            "synapse_id": synapse_id,
                            "graph_run_id": str(observation.graph_run_id),
                        },
                    )
                    if not rows:
                        continue
                    row = rows[0]
                    updated = apply_node_execution_reward(
                        node_role=str(row.get("node_role", "worker")),
                        current_q={
                            "q_action": as_float(row.get("q_action")),
                            "q_hypothesis": as_float(row.get("q_hypothesis")),
                            "q_relevance": as_float(row.get("q_relevance")),
                        },
                        success=success,
                    )
                    effect = await fetch_all(
                        conn,
                        """
                        INSERT INTO outcome_synapse_effects
                            (effect_id, tenant_id, observation_id, synapse_id,
                             source_authority, idempotency_key, policy_version)
                        VALUES
                            (%(effect_id)s, %(tenant_id)s, %(observation_id)s, %(synapse_id)s,
                             %(source_authority)s, %(idempotency_key)s, %(policy_version)s)
                        ON CONFLICT (tenant_id, source_authority, idempotency_key, synapse_id)
                        DO NOTHING
                        RETURNING effect_id::text
                        """,
                        {
                            "effect_id": str(uuid4()),
                            "tenant_id": tenant_id,
                            "observation_id": observation_id,
                            "synapse_id": synapse_id,
                            "source_authority": observation.source_authority,
                            "idempotency_key": observation.idempotency_key,
                            "policy_version": policy_version,
                        },
                    )
                    if not effect:
                        continue
                    await fetch_all(
                        conn,
                        """
                        UPDATE synapses SET
                            q_action=%(q_action)s,
                            q_hypothesis=%(q_hypothesis)s,
                            q_relevance=%(q_relevance)s,
                            updated_at=now()
                        WHERE tenant_id=%(tenant_id)s AND id=%(synapse_id)s
                        RETURNING id::text
                        """,
                        {
                            "q_action": updated.get("q_action", as_float(row.get("q_action"))),
                            "q_hypothesis": updated.get(
                                "q_hypothesis", as_float(row.get("q_hypothesis"))
                            ),
                            "q_relevance": updated.get(
                                "q_relevance", as_float(row.get("q_relevance"))
                            ),
                            "tenant_id": tenant_id,
                            "synapse_id": synapse_id,
                        },
                    )
                    applied.append(synapse_id)
            receipt: JsonDict = {
                "observation_id": observation_id,
                "trace_id": str(observation.trace_id),
                "graph_run_id": str(observation.graph_run_id),
                "policy_version": policy_version,
                "decision": "applied" if applied else "no_change",
                "applied_synapse_ids": str_list_as_json(applied),
            }
            await fetch_all(
                conn,
                """
                INSERT INTO outcome_observations
                    (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
                     observation_kind, source_authority, source_ref, occurred_at,
                     idempotency_key, canonical_digest, policy_version, resolution_receipt)
                VALUES
                    (%(observation_id)s, %(tenant_id)s, %(trace_id)s, %(graph_run_id)s,
                     %(verdict_digest)s, %(observation_kind)s, %(source_authority)s,
                     %(source_ref)s, %(occurred_at)s, %(idempotency_key)s,
                     %(canonical_digest)s, %(policy_version)s, %(resolution_receipt)s)
                RETURNING observation_id
                """,
                {
                    "observation_id": observation_id,
                    "tenant_id": tenant_id,
                    "trace_id": str(observation.trace_id),
                    "graph_run_id": str(observation.graph_run_id),
                    "verdict_digest": observation.verdict_digest,
                    "observation_kind": observation.observation_kind,
                    "source_authority": observation.source_authority,
                    "source_ref": observation.source_ref,
                    "occurred_at": observation.occurred_at.astimezone(timezone.utc),
                    "idempotency_key": observation.idempotency_key,
                    "canonical_digest": canonical_digest,
                    "policy_version": policy_version,
                    "resolution_receipt": Jsonb(receipt),
                },
            )
            return receipt


__all__ = ["OutcomeObservationsMixin"]
