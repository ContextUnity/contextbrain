"""BrainSynapse storage operations — Flat Memory learning-trace store (Postgres).

Implements ``record_synapse`` / ``query_synapses`` / ``update_synapse_q`` /
``decay_synapses`` over the canonical ``synapses`` table.
"""

from __future__ import annotations

from abc import ABC

from contextunity.core.types import JsonDict
from psycopg.types.json import Jsonb

from contextunity.brain.core.config import get_core_config
from contextunity.brain.core.exceptions import SynapseDecayDisabledError
from contextunity.brain.reward_constants import clamp_q
from contextunity.brain.reward_policy import PROCESSED_REWARD_EVENTS_KEY

from .base import PostgresStoreBase
from .helpers import fetch_all


class SynapsesMixin(PostgresStoreBase, ABC):
    """Mixin that adds BrainSynapse CRUD operations to the PostgresStore.

    Requires the host class to provide ``tenant_connection(tenant_id)``.
    """

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
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                """
                INSERT INTO synapses (
                    tenant_id, agent_id, graph_name, graph_run_id, node_id, node_name,
                    action_type, action_data, action_data_ref, context_summary,
                    thought_trace_ref, content_hash, client_id, node_role, fault_class,
                    status, q_action, q_hypothesis, q_relevance, scope_path, metadata
                ) VALUES (
                    %(tenant_id)s, %(agent_id)s, %(graph_name)s, %(graph_run_id)s,
                    %(node_id)s, %(node_name)s, %(action_type)s, %(action_data)s,
                    %(action_data_ref)s, %(context_summary)s, %(thought_trace_ref)s,
                    %(content_hash)s, %(client_id)s, %(node_role)s, %(fault_class)s,
                    %(status)s, %(q_action)s, %(q_hypothesis)s, %(q_relevance)s,
                    %(scope_path)s::ltree, %(metadata)s
                )
                RETURNING id, agent_id, action_type, node_role, status,
                          q_action, q_hypothesis, q_relevance, q_composite,
                          scope_path::text, metadata, created_at, updated_at
                """,
                {
                    "tenant_id": tenant_id,
                    "agent_id": agent_id,
                    "graph_name": graph_name,
                    "graph_run_id": graph_run_id,
                    "node_id": node_id,
                    "node_name": node_name,
                    "action_type": action_type,
                    "action_data": Jsonb(action_data or {}),
                    "action_data_ref": action_data_ref,
                    "context_summary": context_summary,
                    "thought_trace_ref": thought_trace_ref,
                    "content_hash": content_hash,
                    "client_id": client_id,
                    "node_role": node_role,
                    "fault_class": fault_class,
                    "status": status,
                    "q_action": clamp_q(q_action),
                    "q_hypothesis": clamp_q(q_hypothesis),
                    "q_relevance": clamp_q(q_relevance),
                    "scope_path": scope_path,
                    "metadata": Jsonb(metadata or {}),
                },
            )
        return rows[0]

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
        production-learning set (``active``, ``confirmed``) so callers never
        have to spell that out for the common case.
        """
        status_clause = (
            "status = %(status)s" if status is not None else "status IN ('active', 'confirmed')"
        )
        async with await self.tenant_connection(tenant_id) as conn:
            return await fetch_all(
                conn,
                f"""
                SELECT id, agent_id, graph_name, graph_run_id, node_id, node_name,
                       action_type, action_data, action_data_ref, context_summary,
                       thought_trace_ref, content_hash, node_role, fault_class, status,
                       q_action, q_hypothesis, q_relevance, q_composite,
                       scope_path::text, metadata, created_at, updated_at
                FROM synapses
                WHERE tenant_id = %(tenant_id)s
                  AND {status_clause}
                  AND q_composite >= %(min_q)s
                  AND (%(action_type)s::text IS NULL OR action_type = %(action_type)s::text)
                  AND (%(agent_id)s::text IS NULL OR agent_id = %(agent_id)s::text)
                  AND (%(node_role)s::text IS NULL OR node_role = %(node_role)s::text)
                  AND (%(scope_path)s::text IS NULL OR scope_path <@ %(scope_path)s::text::ltree)
                ORDER BY q_composite DESC, updated_at DESC
                LIMIT %(limit)s
                """,
                {
                    "tenant_id": tenant_id,
                    "status": status,
                    "min_q": min_q,
                    "action_type": action_type,
                    "agent_id": agent_id,
                    "node_role": node_role,
                    "scope_path": scope_path,
                    "limit": limit,
                },
            )

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

        Unset (``None``) fields are left unchanged via ``COALESCE``.
        ``metadata_patch`` is shallow-merged (``||``) into existing metadata.
        Returns ``None`` if no row with that id exists for this tenant.

        ``idempotency_key`` (a ``review_id`` or ``event_id``) makes replay
        safe: the ``WHERE`` clause excludes rows where this key already
        appears in ``metadata.processed_reward_events`` (a single atomic
        statement, race-safe under concurrent replay), and the key is
        appended to that list on a successful apply. A 0-row result can mean
        either "not found" or "already applied" — the fallback SELECT
        disambiguates and returns the *current* unchanged values for the
        latter instead of silently reporting not-found.
        """
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                f"""
                UPDATE synapses SET
                    q_action = COALESCE(%(q_action)s, q_action),
                    q_hypothesis = COALESCE(%(q_hypothesis)s, q_hypothesis),
                    q_relevance = COALESCE(%(q_relevance)s, q_relevance),
                    fault_class = COALESCE(%(fault_class)s, fault_class),
                    status = COALESCE(%(status)s, status),
                    metadata = (metadata || %(metadata_patch)s) || jsonb_build_object(
                        '{PROCESSED_REWARD_EVENTS_KEY}',
                        CASE
                            WHEN %(idempotency_key)s::text IS NULL
                                THEN COALESCE(metadata->'{PROCESSED_REWARD_EVENTS_KEY}', '[]'::jsonb)
                            ELSE COALESCE(metadata->'{PROCESSED_REWARD_EVENTS_KEY}', '[]'::jsonb)
                                || to_jsonb(ARRAY[%(idempotency_key)s]::text[])
                        END
                    ),
                    updated_at = now()
                WHERE id = %(synapse_id)s AND tenant_id = %(tenant_id)s
                  AND (
                    %(idempotency_key)s::text IS NULL
                    OR NOT (COALESCE(metadata->'{PROCESSED_REWARD_EVENTS_KEY}', '[]'::jsonb) ? %(idempotency_key)s::text)
                  )
                RETURNING id, q_action, q_hypothesis, q_relevance, q_composite, updated_at
                """,
                {
                    "tenant_id": tenant_id,
                    "synapse_id": synapse_id,
                    "q_action": clamp_q(q_action) if q_action is not None else None,
                    "q_hypothesis": clamp_q(q_hypothesis) if q_hypothesis is not None else None,
                    "q_relevance": clamp_q(q_relevance) if q_relevance is not None else None,
                    "fault_class": fault_class,
                    "status": status,
                    "metadata_patch": Jsonb(metadata_patch or {}),
                    "idempotency_key": idempotency_key,
                },
            )
            if rows:
                return rows[0]
            if idempotency_key is None:
                return None
            # 0 rows with an idempotency_key given: disambiguate "not found"
            # from "already applied" (the WHERE clause excludes the latter).
            existing = await fetch_all(
                conn,
                """
                SELECT id, q_action, q_hypothesis, q_relevance, q_composite, updated_at
                FROM synapses WHERE id = %(synapse_id)s AND tenant_id = %(tenant_id)s
                """,
                {"synapse_id": synapse_id, "tenant_id": tenant_id},
            )
        return existing[0] if existing else None

    async def decay_synapses(self, *, tenant_id: str, factor: float = 0.99) -> int:
        """Phase 5 Consolidation Cycle Q-decay hook — disabled by default.

        Raises:
            SynapseDecayDisabledError: ``brain.yml: synapses.decay_enabled`` is ``False``.
        """
        if not get_core_config().synapses.decay_enabled:
            raise SynapseDecayDisabledError(tenant_id=tenant_id)
        raise NotImplementedError("Synapse decay ships with the Phase 5 Consolidation Cycle")


__all__ = ["SynapsesMixin"]
