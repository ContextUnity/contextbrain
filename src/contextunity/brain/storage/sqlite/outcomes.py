"""Transactional OutcomeObservation resolution for the SQLite backend."""

from __future__ import annotations

import hashlib
import json
from datetime import timezone
from uuid import uuid4

from contextunity.core.narrowing import as_float, str_list_as_json
from contextunity.core.parsing import json_dumps, json_loads
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.payloads.outcomes import OutcomeObservationPayload
from contextunity.brain.reward_constants import q_composite
from contextunity.brain.reward_policy import apply_node_execution_reward, is_trainable_tenant

from .connection import SqliteConnectionMixin


def _observation_digest(observation: OutcomeObservationPayload) -> str:
    wire = observation.model_dump(mode="json")
    encoded = json.dumps(wire, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class OutcomeObservationsMixin(SqliteConnectionMixin):
    async def import_outcome_observation_record(self, *, tenant_id: str, record: JsonDict) -> None:
        with self._get_connection() as db:
            existing = db.execute(
                "SELECT canonical_digest FROM outcome_observations WHERE tenant_id=? AND observation_id=?",
                (tenant_id, str(record["observation_id"])),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != str(record["canonical_digest"]):
                    raise BrainValidationError("conflicting imported outcome observation")
                return
            db.execute(
                "INSERT INTO outcome_observations (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest, observation_kind, source_authority, source_ref, occurred_at, idempotency_key, canonical_digest, policy_version, resolution_receipt, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(record["observation_id"]),
                    tenant_id,
                    str(record["trace_id"]),
                    str(record["graph_run_id"]),
                    str(record["verdict_digest"]),
                    str(record["observation_kind"]),
                    str(record["source_authority"]),
                    str(record["source_ref"]),
                    str(record["occurred_at"]),
                    str(record["idempotency_key"]),
                    str(record["canonical_digest"]),
                    str(record["policy_version"]),
                    json_dumps(record["resolution_receipt"]),
                    str(record["created_at"]),
                ),
            )
            db.commit()

    async def resolve_outcome_observation(
        self,
        *,
        tenant_id: str,
        observation: OutcomeObservationPayload,
        policy_version: str,
    ) -> JsonDict:
        digest = _observation_digest(observation)
        with self._get_connection() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                "SELECT canonical_digest, resolution_receipt FROM outcome_observations "
                "WHERE tenant_id = ? AND source_authority = ? AND idempotency_key = ?",
                (tenant_id, observation.source_authority, observation.idempotency_key),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != digest:
                    raise BrainValidationError("conflicting outcome observation idempotency key")
                receipt_raw = json_loads(str(existing[1]))
                if not is_json_dict(receipt_raw):
                    raise BrainValidationError("stored outcome resolution receipt is malformed")
                duplicate: JsonDict = dict(receipt_raw)
                duplicate["decision"] = "duplicate"
                db.commit()
                return duplicate

            trace = db.execute(
                "SELECT graph_run_id, terminal_status, terminal_reason, final_verdict "
                "FROM execution_traces WHERE tenant_id = ? AND id = ?",
                (tenant_id, str(observation.trace_id)),
            ).fetchone()
            if trace is None:
                raise BrainValidationError("outcome observation terminal trace not found")
            if str(trace[0]) != str(observation.graph_run_id):
                raise BrainValidationError("outcome observation graph run mismatch")
            verdict = json_loads(str(trace[3]))
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
            fault_class = verdict.get("fault_class")
            eligible = is_trainable_tenant(tenant_id) and (
                success or (verified_failure and fault_class == "agent_fault")
            )
            observation_id = str(uuid4())
            applied: list[str] = []
            if eligible:
                for synapse_id in candidate_ids:
                    row = db.execute(
                        "SELECT node_role, q_action, q_hypothesis, q_relevance "
                        "FROM synapses WHERE tenant_id = ? AND id = ? AND graph_run_id = ?",
                        (tenant_id, synapse_id, str(observation.graph_run_id)),
                    ).fetchone()
                    if row is None:
                        continue
                    updated = apply_node_execution_reward(
                        node_role=str(row[0]),
                        current_q={
                            "q_action": as_float(row[1]),
                            "q_hypothesis": as_float(row[2]),
                            "q_relevance": as_float(row[3]),
                        },
                        success=success,
                    )
                    qa = float(updated.get("q_action", as_float(row[1])))
                    qh = float(updated.get("q_hypothesis", as_float(row[2])))
                    qr = float(updated.get("q_relevance", as_float(row[3])))
                    effect = db.execute(
                        "INSERT OR IGNORE INTO outcome_synapse_effects "
                        "(effect_id, tenant_id, observation_id, synapse_id, source_authority, "
                        "idempotency_key, policy_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            str(uuid4()),
                            tenant_id,
                            observation_id,
                            synapse_id,
                            observation.source_authority,
                            observation.idempotency_key,
                            policy_version,
                        ),
                    )
                    if effect.rowcount != 1:
                        continue
                    db.execute(
                        "UPDATE synapses SET q_action=?, q_hypothesis=?, q_relevance=?, "
                        "q_composite=?, updated_at=datetime('now') "
                        "WHERE tenant_id=? AND id=?",
                        (qa, qh, qr, q_composite(qa, qh, qr), tenant_id, synapse_id),
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
            db.execute(
                "INSERT INTO outcome_observations "
                "(observation_id, tenant_id, trace_id, graph_run_id, verdict_digest, "
                "observation_kind, source_authority, source_ref, occurred_at, idempotency_key, "
                "canonical_digest, policy_version, resolution_receipt) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    observation_id,
                    tenant_id,
                    str(observation.trace_id),
                    str(observation.graph_run_id),
                    observation.verdict_digest,
                    observation.observation_kind,
                    observation.source_authority,
                    observation.source_ref,
                    observation.occurred_at.astimezone(timezone.utc).isoformat(),
                    observation.idempotency_key,
                    digest,
                    policy_version,
                    json_dumps(receipt),
                ),
            )
            db.commit()
            return receipt


__all__ = ["OutcomeObservationsMixin"]
