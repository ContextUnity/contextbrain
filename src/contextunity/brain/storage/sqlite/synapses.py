"""BrainSynapse storage operations — Flat Memory Phase B (SQLite implementation).

Contract-compatible with ``postgres/store/synapses.py``. SQLite has no
generated-column support in this codepath, so ``q_composite`` is computed in
Python via the shared ``reward_constants.q_composite`` formula and stored
alongside the three inputs, instead of being derived by the database.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone

from contextunity.core.narrowing import as_float, optional_str_field
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core.config import get_core_config
from contextunity.brain.core.exceptions import SynapseDecayDisabledError
from contextunity.brain.reward_constants import clamp_q
from contextunity.brain.reward_constants import q_composite as compute_q_composite
from contextunity.brain.reward_policy import is_already_processed, mark_processed

from .codecs import fetchone_row, json_dumps, json_loads, sqlite_cell
from .connection import SqliteConnectionMixin

_SYNAPSE_COLUMNS = (
    "id",
    "tenant_id",
    "agent_id",
    "graph_name",
    "graph_run_id",
    "node_id",
    "node_name",
    "action_type",
    "action_data",
    "action_data_ref",
    "context_summary",
    "thought_trace_ref",
    "content_hash",
    "node_role",
    "fault_class",
    "status",
    "q_action",
    "q_hypothesis",
    "q_relevance",
    "q_composite",
    "scope_path",
    "metadata",
    "created_at",
    "updated_at",
)


def _row_to_synapse_dict(row: sqlite3.Row) -> JsonDict:
    out: JsonDict = {}
    for column in _SYNAPSE_COLUMNS:
        if column in ("action_data", "metadata"):
            raw = sqlite_cell(row, column)
            loaded = json_loads(raw if isinstance(raw, str) else None)
            out[column] = loaded if is_json_dict(loaded) else {}
        elif column in ("q_action", "q_hypothesis", "q_relevance", "q_composite"):
            out[column] = as_float(sqlite_cell(row, column))
        else:
            cell = sqlite_cell(row, column)
            out[column] = cell if isinstance(cell, str) else None
    return out


class SynapsesMixin(SqliteConnectionMixin):
    """SQLite BrainSynapse operations matching the Postgres contract."""

    async def record_synapse(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        action_type: str,
        action_data: JsonDict | None = None,
        action_data_ref: str | None = None,
        thought_trace_ref: str | None = None,
        content_hash: str | None = None,
        graph_name: str | None = None,
        graph_run_id: str | None = None,
        node_id: str | None = None,
        node_name: str | None = None,
        node_role: str = "worker",
        scope_path: str | None = None,
        context_summary: str | None = None,
        client_id: str | None = None,
        fault_class: str | None = None,
        status: str = "active",
        q_action: float = 0.5,
        q_hypothesis: float = 0.5,
        q_relevance: float = 0.5,
        metadata: JsonDict | None = None,
    ) -> JsonDict:
        """Insert one BrainSynapse row; returns the generated id/Q-values/timestamp."""
        record_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        clamped_action = clamp_q(q_action)
        clamped_hypothesis = clamp_q(q_hypothesis)
        clamped_relevance = clamp_q(q_relevance)
        composite = compute_q_composite(clamped_action, clamped_hypothesis, clamped_relevance)

        with self._get_connection() as db:
            _ = db.execute(
                """
                INSERT INTO synapses (
                    id, tenant_id, agent_id, graph_name, graph_run_id, node_id, node_name,
                    action_type, action_data, action_data_ref, context_summary,
                    thought_trace_ref, content_hash, client_id, node_role, fault_class,
                    status, q_action, q_hypothesis, q_relevance, q_composite, scope_path,
                    metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    tenant_id,
                    agent_id,
                    graph_name,
                    graph_run_id,
                    node_id,
                    node_name,
                    action_type,
                    json_dumps(action_data or {}),
                    action_data_ref,
                    context_summary,
                    thought_trace_ref,
                    content_hash,
                    client_id,
                    node_role,
                    fault_class,
                    status,
                    clamped_action,
                    clamped_hypothesis,
                    clamped_relevance,
                    composite,
                    scope_path,
                    json_dumps(metadata or {}),
                    now,
                    now,
                ),
            )
            db.commit()

        return {
            "id": record_id,
            "agent_id": agent_id,
            "action_type": action_type,
            "node_role": node_role,
            "status": status,
            "q_action": clamped_action,
            "q_hypothesis": clamped_hypothesis,
            "q_relevance": clamped_relevance,
            "q_composite": composite,
            "scope_path": scope_path,
            "metadata": metadata or {},
            "created_at": now,
            "updated_at": now,
        }

    async def query_synapses(
        self,
        *,
        tenant_id: str,
        action_type: str | None = None,
        agent_id: str | None = None,
        node_role: str | None = None,
        status: str | None = None,
        scope_path: str | None = None,
        min_q: float = 0.6,
        limit: int = 5,
    ) -> list[JsonDict]:
        """Query Synapses ranked by ``q_composite DESC``, bounded by ``limit``.

        ``status`` filters exactly when given; omitted, it defaults to the
        production-learning set (``active``, ``confirmed``).
        """
        clauses = ["tenant_id = ?", "q_composite >= ?"]
        params: list[object] = [tenant_id, min_q]

        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        else:
            clauses.append("status IN ('active', 'confirmed')")

        if action_type is not None:
            clauses.append("action_type = ?")
            params.append(action_type)
        if agent_id is not None:
            clauses.append("agent_id = ?")
            params.append(agent_id)
        if node_role is not None:
            clauses.append("node_role = ?")
            params.append(node_role)
        if scope_path is not None:
            clauses.append("(scope_path = ? OR substr(scope_path, 1, length(?) + 1) = ? || '.')")
            params.extend([scope_path, scope_path, scope_path])

        params.append(limit)

        with self._get_connection() as db:
            cursor = db.execute(
                f"""
                SELECT {", ".join(_SYNAPSE_COLUMNS)}
                FROM synapses
                WHERE {" AND ".join(clauses)}
                ORDER BY q_composite DESC, updated_at DESC
                LIMIT ?
                """,
                params,
            )
            rows: list[sqlite3.Row] = list(cursor.fetchall())

        return [_row_to_synapse_dict(row) for row in rows]

    async def update_synapse_q(
        self,
        *,
        tenant_id: str,
        synapse_id: str,
        q_action: float | None = None,
        q_hypothesis: float | None = None,
        q_relevance: float | None = None,
        fault_class: str | None = None,
        status: str | None = None,
        metadata_patch: JsonDict | None = None,
        idempotency_key: str | None = None,
    ) -> JsonDict | None:
        """Update Q-values/fault/status on one tenant-owned Synapse.

        Unset (``None``) fields are left unchanged. ``metadata_patch`` is
        shallow-merged into existing metadata (never replaces it wholesale).
        Returns ``None`` if no row with that id exists for this tenant.

        ``idempotency_key`` (a ``review_id`` or ``event_id``) makes replay
        safe: if this key was already applied to this Synapse (recorded in
        ``metadata.processed_reward_events``), the call is a no-op that
        returns the *current* unchanged values instead of re-applying.
        """
        now = datetime.now(timezone.utc).isoformat()

        with self._get_connection() as db:
            cursor = db.execute(
                f"SELECT {', '.join(_SYNAPSE_COLUMNS)} FROM synapses WHERE id = ? AND tenant_id = ?",
                (synapse_id, tenant_id),
            )
            existing_row = fetchone_row(cursor)
            if existing_row is None:
                return None
            existing = _row_to_synapse_dict(existing_row)

            existing_metadata = existing["metadata"]
            current_metadata = dict(existing_metadata) if is_json_dict(existing_metadata) else {}

            if is_already_processed(current_metadata, idempotency_key):
                return {
                    "id": synapse_id,
                    "q_action": as_float(existing["q_action"]),
                    "q_hypothesis": as_float(existing["q_hypothesis"]),
                    "q_relevance": as_float(existing["q_relevance"]),
                    "q_composite": as_float(existing["q_composite"]),
                    "updated_at": optional_str_field(existing, "updated_at") or now,
                }

            new_action = (
                clamp_q(q_action) if q_action is not None else as_float(existing["q_action"])
            )
            new_hypothesis = (
                clamp_q(q_hypothesis)
                if q_hypothesis is not None
                else as_float(existing["q_hypothesis"])
            )
            new_relevance = (
                clamp_q(q_relevance)
                if q_relevance is not None
                else as_float(existing["q_relevance"])
            )
            new_composite = compute_q_composite(new_action, new_hypothesis, new_relevance)
            new_fault_class = (
                fault_class
                if fault_class is not None
                else optional_str_field(existing, "fault_class")
            )
            new_status = (
                status
                if status is not None
                else (optional_str_field(existing, "status") or "active")
            )

            merged_metadata = dict(current_metadata)
            if metadata_patch:
                merged_metadata.update(metadata_patch)
            merged_metadata = mark_processed(merged_metadata, idempotency_key)

            _ = db.execute(
                """
                UPDATE synapses SET
                    q_action = ?, q_hypothesis = ?, q_relevance = ?, q_composite = ?,
                    fault_class = ?, status = ?, metadata = ?, updated_at = ?
                WHERE id = ? AND tenant_id = ?
                """,
                (
                    new_action,
                    new_hypothesis,
                    new_relevance,
                    new_composite,
                    new_fault_class,
                    new_status,
                    json_dumps(merged_metadata),
                    now,
                    synapse_id,
                    tenant_id,
                ),
            )
            db.commit()

        return {
            "id": synapse_id,
            "q_action": new_action,
            "q_hypothesis": new_hypothesis,
            "q_relevance": new_relevance,
            "q_composite": new_composite,
            "updated_at": now,
        }

    async def decay_synapses(self, *, tenant_id: str, factor: float = 0.99) -> int:
        """Phase 3/5-ready Q-decay hook — raises while disabled (default).

        Raises:
            SynapseDecayDisabledError: ``brain.yml: synapses.decay_enabled`` is ``False``.
        """
        if not get_core_config().synapses.decay_enabled:
            raise SynapseDecayDisabledError(tenant_id=tenant_id)
        raise NotImplementedError("Synapse decay ships with the Phase 3/5 Dream Cycle")


__all__ = ["SynapsesMixin"]
