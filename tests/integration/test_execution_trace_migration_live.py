"""Live PostgreSQL proof for the reversible execution-trace migration."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, errors, sql

from contextunity.brain.storage.postgres.schema import (
    build_column_backfill_sql,
    build_rls_sql,
    build_schema_sql,
)

pytestmark = [pytest.mark.integration_live, pytest.mark.asyncio]

_DSN = (os.environ.get("BRAIN_TEST_DSN") or "").strip()


def _migration_operations() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Load every execution-trace schema migration in upgrade/downgrade order."""
    versions = Path(__file__).resolve().parents[2] / "migrations" / "versions"
    migration_paths = (
        versions / "0015_execution_traces.py",
        versions / "0020_execution_trace_control_evidence.py",
        versions / "0021_execution_trace_artifacts.py",
        versions / "0022_outcome_observations.py",
    )
    recorded: list[str] = []
    fake_op = types.SimpleNamespace(execute=recorded.append)
    fake_alembic = types.ModuleType("alembic")
    fake_alembic.op = fake_op
    real_alembic = sys.modules.get("alembic")
    sys.modules["alembic"] = fake_alembic
    try:
        modules: list[types.ModuleType] = []
        for index, migration_path in enumerate(migration_paths):
            spec = importlib.util.spec_from_file_location(
                f"_trace_migration_live_{index}",
                migration_path,
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            modules.append(module)
        for module in modules:
            module.upgrade()
        upgrades = tuple(recorded)
        recorded.clear()
        for module in reversed(modules):
            module.downgrade()
        downgrades = tuple(recorded)
    finally:
        if real_alembic is None:
            del sys.modules["alembic"]
        else:
            sys.modules["alembic"] = real_alembic
    assert upgrades and downgrades
    return upgrades, downgrades


async def _prepare_legacy_trace_table(
    conn: AsyncConnection[object],
    *,
    policy_name: str | None = None,
) -> None:
    _ = await conn.execute(
        """
        CREATE TABLE event_journal (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            session_id TEXT NULL,
            user_id TEXT NULL,
            graph_name TEXT NULL,
            tool_calls JSONB NOT NULL DEFAULT '[]'::jsonb,
            token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,
            timing_ms INTEGER NULL,
            security_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            provenance TEXT[] NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_id UUID DEFAULT gen_random_uuid(),
            event_type TEXT NOT NULL DEFAULT 'trace.logged',
            severity TEXT NOT NULL DEFAULT 'info',
            status TEXT NOT NULL DEFAULT 'recorded',
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            source_refs JSONB NOT NULL DEFAULT '[]'::jsonb
        );
        CREATE TABLE synapses (
            id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL
        );
        CREATE INDEX event_journal_tenant_idx ON event_journal (tenant_id);
        CREATE INDEX event_journal_agent_idx ON event_journal (agent_id);
        CREATE INDEX event_journal_session_idx ON event_journal (session_id);
        CREATE INDEX event_journal_created_idx ON event_journal (created_at DESC);
        CREATE INDEX event_journal_tenant_created_idx
            ON event_journal (tenant_id, created_at DESC);
        ALTER TABLE event_journal ENABLE ROW LEVEL SECURITY;
        ALTER TABLE event_journal FORCE ROW LEVEL SECURITY;
        """
    )
    if policy_name is not None:
        _ = await conn.execute(
            sql.SQL("""
            CREATE POLICY {} ON event_journal
            USING (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            )
            WITH CHECK (
                (tenant_id = current_setting('app.current_tenant', true)
                 OR current_setting('app.current_tenant', true) = '*')
                AND (
                    current_setting('app.current_user', true) = '*'
                    OR user_id IS NULL
                    OR user_id = current_setting('app.current_user', true)
                )
            );
            """).format(sql.Identifier(policy_name))
        )


async def _table_signature(conn: AsyncConnection[object]) -> tuple[list[tuple[object, ...]], ...]:
    columns = await (
        await conn.execute(
            """
            SELECT column_name, data_type, is_nullable, COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'execution_traces'
            ORDER BY column_name
            """
        )
    ).fetchall()
    indexes = await (
        await conn.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = current_schema() AND tablename = 'execution_traces'
            ORDER BY indexname
            """
        )
    ).fetchall()
    constraints = await (
        await conn.execute(
            """
            SELECT conname, contype FROM pg_constraint
            WHERE conrelid = 'execution_traces'::regclass
            ORDER BY conname
            """
        )
    ).fetchall()
    policies = await (
        await conn.execute(
            """
            SELECT policyname FROM pg_policies
            WHERE schemaname = current_schema() AND tablename = 'execution_traces'
            ORDER BY policyname
            """
        )
    ).fetchall()
    return columns, indexes, constraints, policies


async def _artifact_table_signature(
    conn: AsyncConnection[object],
) -> tuple[list[tuple[object, ...]], ...]:
    columns = await (
        await conn.execute(
            """
            SELECT column_name, data_type, is_nullable, COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'execution_trace_artifacts'
            ORDER BY column_name
            """
        )
    ).fetchall()
    indexes = await (
        await conn.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'execution_trace_artifacts'
            ORDER BY indexname
            """
        )
    ).fetchall()
    constraints = await (
        await conn.execute(
            """
            SELECT conname, contype FROM pg_constraint
            WHERE conrelid = 'execution_trace_artifacts'::regclass
            ORDER BY conname
            """
        )
    ).fetchall()
    policies = await (
        await conn.execute(
            """
            SELECT policyname FROM pg_policies
            WHERE schemaname = current_schema()
              AND tablename = 'execution_trace_artifacts'
            ORDER BY policyname
            """
        )
    ).fetchall()
    return columns, indexes, constraints, policies


async def _outcome_table_signature(
    conn: AsyncConnection[object], table_name: str
) -> tuple[list[tuple[object, ...]], ...]:
    columns = await (
        await conn.execute(
            """
            SELECT column_name, data_type, is_nullable, COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY column_name
            """,
            (table_name,),
        )
    ).fetchall()
    indexes = await (
        await conn.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = current_schema() AND tablename = %s
            ORDER BY indexname
            """,
            (table_name,),
        )
    ).fetchall()
    constraints = await (
        await conn.execute(
            """
            SELECT conname, contype FROM pg_constraint
            WHERE conrelid = to_regclass(current_schema() || '.' || %s)
            ORDER BY conname
            """,
            (table_name,),
        )
    ).fetchall()
    policies = await (
        await conn.execute(
            """
            SELECT policyname FROM pg_policies
            WHERE schemaname = current_schema() AND tablename = %s
            ORDER BY policyname
            """,
            (table_name,),
        )
    ).fetchall()
    grants = await (
        await conn.execute(
            """
            SELECT grantee, privilege_type FROM information_schema.role_table_grants
            WHERE table_schema = current_schema() AND table_name = %s
              AND grantee IN ('brain_app', 'brain_admin')
            ORDER BY grantee, privilege_type
            """,
            (table_name,),
        )
    ).fetchall()
    return columns, indexes, constraints, policies, grants


async def test_upgrade_downgrade_reupgrade_and_fresh_reconciliation() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sqls, downgrade_sqls = _migration_operations()
    migrated_schema = f"trace_migrated_{uuid4().hex}"
    fresh_schema = f"trace_fresh_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    try:
        _ = await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(migrated_schema), sql.Identifier(migrated_schema)
            )
        )
        await _prepare_legacy_trace_table(conn)
        trace_id = uuid4()
        _ = await conn.execute(
            "INSERT INTO event_journal (id, tenant_id, agent_id) VALUES (%s, %s, %s)",
            (trace_id, "tenant-a", "agent-a"),
        )

        for statement in upgrade_sqls:
            _ = await conn.execute(statement)
        row = await (
            await conn.execute(
                "SELECT id, trace_schema_version, terminal_status FROM execution_traces"
            )
        ).fetchone()
        assert row == (trace_id, "legacy_v0", None)

        for statement in downgrade_sqls:
            _ = await conn.execute(statement)
        assert await (
            await conn.execute("SELECT graph_run_id FROM event_journal WHERE id = %s", (trace_id,))
        ).fetchone() == (None,)
        for statement in upgrade_sqls:
            _ = await conn.execute(statement)
        migrated_signature = await _table_signature(conn)
        migrated_artifact_signature = await _artifact_table_signature(conn)
        migrated_outcome_signature = await _outcome_table_signature(conn, "outcome_observations")
        migrated_effect_signature = await _outcome_table_signature(conn, "outcome_synapse_effects")
        effect_constraints = {str(row[0]) for row in migrated_effect_signature[2]}
        assert "outcome_synapse_effects_observation_scope_fk" in effect_constraints
        assert "outcome_synapse_effects_synapse_scope_fk" in effect_constraints

        observation_id = uuid4()
        synapse_id = uuid4()
        foreign_synapse_id = uuid4()
        foreign_trace_id = uuid4()
        _ = await conn.execute(
            "INSERT INTO synapses (id, tenant_id) VALUES (%s, %s), (%s, %s)",
            (synapse_id, "tenant-a", foreign_synapse_id, "tenant-b"),
        )
        _ = await conn.execute(
            "INSERT INTO execution_traces (id, tenant_id, agent_id, graph_run_id) "
            "VALUES (%s, %s, %s, %s)",
            (foreign_trace_id, "tenant-b", "agent-b", uuid4()),
        )
        with pytest.raises(errors.ForeignKeyViolation):
            _ = await conn.execute(
                """
                INSERT INTO outcome_observations
                    (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
                     observation_kind, source_authority, source_ref, occurred_at,
                     idempotency_key, canonical_digest, policy_version, resolution_receipt)
                VALUES (%s, 'tenant-a', %s, %s, %s, 'neutral', 'operator_review/v1',
                        'review:cross-trace', now(), 'cross-trace', %s,
                        'contextunity.outcome-resolution/v1', '{}'::jsonb)
                """,
                (uuid4(), foreign_trace_id, uuid4(), "c" * 64, "d" * 64),
            )
        _ = await conn.execute(
            """
            INSERT INTO outcome_observations
                (observation_id, tenant_id, trace_id, graph_run_id, verdict_digest,
                 observation_kind, source_authority, source_ref, occurred_at,
                 idempotency_key, canonical_digest, policy_version, resolution_receipt)
            VALUES (%s, %s, %s, %s, %s, 'neutral', 'operator_review/v1',
                    'review:migration-rls', now(), 'migration-rls', %s,
                    'contextunity.outcome-resolution/v1', '{}'::jsonb)
            """,
            (observation_id, "tenant-a", trace_id, uuid4(), "a" * 64, "b" * 64),
        )
        with pytest.raises(errors.ForeignKeyViolation):
            _ = await conn.execute(
                """
                INSERT INTO outcome_synapse_effects
                    (effect_id, tenant_id, observation_id, synapse_id, source_authority,
                     idempotency_key, policy_version)
                VALUES (%s, 'tenant-a', %s, %s, 'operator_review/v1',
                        'cross-synapse', 'contextunity.outcome-resolution/v1')
                """,
                (uuid4(), observation_id, foreign_synapse_id),
            )
        _ = await conn.execute(
            """
            INSERT INTO outcome_synapse_effects
                (effect_id, tenant_id, observation_id, synapse_id, source_authority,
                 idempotency_key, policy_version)
            VALUES (%s, %s, %s, %s, 'operator_review/v1', 'migration-rls',
                    'contextunity.outcome-resolution/v1')
            """,
            (uuid4(), "tenant-a", observation_id, synapse_id),
        )
        _ = await conn.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO brain_app").format(sql.Identifier(migrated_schema))
        )
        _ = await conn.execute("SET ROLE brain_app")
        try:
            _ = await conn.execute("SET app.current_tenant = 'tenant-b'")
            assert await (
                await conn.execute("SELECT COUNT(*) FROM outcome_observations")
            ).fetchone() == (0,)
            assert await (
                await conn.execute("SELECT COUNT(*) FROM outcome_synapse_effects")
            ).fetchone() == (0,)
        finally:
            _ = await conn.execute("RESET ROLE")

        _ = await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(fresh_schema), sql.Identifier(fresh_schema)
            )
        )
        for statement in build_schema_sql(vector_dim=8):
            _ = await conn.execute(statement)
        for statement in build_column_backfill_sql():
            _ = await conn.execute(statement)
        for statement in build_rls_sql():
            if "execution_trace" in statement or "outcome_" in statement:
                _ = await conn.execute(statement)
        fresh_signature = await _table_signature(conn)
        fresh_artifact_signature = await _artifact_table_signature(conn)
        fresh_outcome_signature = await _outcome_table_signature(conn, "outcome_observations")
        fresh_effect_signature = await _outcome_table_signature(conn, "outcome_synapse_effects")

        assert migrated_signature == fresh_signature
        assert migrated_artifact_signature == fresh_artifact_signature
        assert migrated_outcome_signature == fresh_outcome_signature
        assert migrated_effect_signature == fresh_effect_signature
    finally:
        _ = await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(migrated_schema))
        )
        _ = await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(fresh_schema))
        )
        await conn.close()


@pytest.mark.parametrize(
    "predecessor_policy",
    ["agent_traces_tenant_isolation", "event_journal_tenant_isolation"],
)
async def test_upgrade_normalizes_supported_predecessor_policy_shapes(
    predecessor_policy: str,
) -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sqls, _ = _migration_operations()
    schema = f"trace_policy_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN, autocommit=True)
    try:
        _ = await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(schema),
                sql.Identifier(schema),
            )
        )
        await _prepare_legacy_trace_table(conn, policy_name=predecessor_policy)
        for statement in upgrade_sqls:
            _ = await conn.execute(statement)
        policies = await (
            await conn.execute(
                """
                SELECT policyname FROM pg_policies
                WHERE schemaname = current_schema()
                  AND tablename = 'execution_traces'
                ORDER BY policyname
                """
            )
        ).fetchall()
        assert policies == [("execution_traces_tenant_isolation",)]
    finally:
        _ = await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        await conn.close()


async def test_unknown_generic_event_rolls_back_migration() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    upgrade_sqls, _ = _migration_operations()
    schema = f"trace_reject_{uuid4().hex}"
    conn = await AsyncConnection.connect(_DSN)
    try:
        _ = await conn.execute(
            sql.SQL("CREATE SCHEMA {}; SET search_path TO {}, public").format(
                sql.Identifier(schema), sql.Identifier(schema)
            )
        )
        await _prepare_legacy_trace_table(conn)
        _ = await conn.execute(
            """
            INSERT INTO event_journal (id, tenant_id, agent_id, event_type)
            VALUES (%s, 'tenant-a', 'agent-a', 'unknown.event')
            """,
            (uuid4(),),
        )
        await conn.commit()
        with pytest.raises(errors.RaiseException, match="unmapped generic event row"):
            _ = await conn.execute(upgrade_sqls[0])
        await conn.rollback()
        assert await (
            await conn.execute("SELECT to_regclass('event_journal')::text")
        ).fetchone() == ("event_journal",)
    finally:
        await conn.rollback()
        await conn.set_autocommit(True)
        _ = await conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        await conn.close()
