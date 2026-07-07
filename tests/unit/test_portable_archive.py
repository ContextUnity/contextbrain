"""Tests for Portable Archive v1 — export, validation, import, embeddings.

Run: ``uv run pytest services/brain/tests/unit/test_portable_archive.py -v``
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from contextunity.core.exceptions import StorageError

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.portable import (
    BrainPortableArchiveReader,
    BrainPortableArchiveWriter,
    FactRecord,
    PortableManifest,
    SynapseRecord,
    import_portable_archive,
    parse_record,
)
from contextunity.brain.storage.sqlite import SqliteBrainStore

BRAIN_TEST_DSN = os.environ.get("BRAIN_TEST_DSN")


@pytest.fixture
def store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(
        db_path=str(tmp_path / "export_test.sqlite3"),
        vector_dim=8,
    )


@pytest.fixture
def run():
    def _run(coro):
        return asyncio.run(coro)

    return _run


TENANT = "export-tenant"


class TestParseRecord:
    def test_parse_valid(self):
        line = json.dumps(
            {
                "type": "fact",
                "tenant_id": "t",
                "user_id": "u",
                "fact_key": "k",
                "fact_value": "v",
            }
        )
        rec = parse_record(line)
        assert isinstance(rec, FactRecord)

    def test_parse_synapse_record(self):
        line = json.dumps(
            {
                "type": "synapse",
                "tenant_id": "t",
                "id": "syn-1",
                "agent_id": "agent-1",
                "action_type": "tool_call",
                "q_composite": 0.72,
                "created_at": "2026-07-06T00:00:00+00:00",
                "updated_at": "2026-07-06T00:00:00+00:00",
            }
        )
        rec = parse_record(line)
        assert isinstance(rec, SynapseRecord)
        assert rec.q_composite == 0.72

    def test_parse_unknown_type(self):
        with pytest.raises(BrainValidationError, match="Unknown record type"):
            parse_record('{"type": "alien"}')


# ── Export ────────────────────────────────────────────────────────


class TestExport:
    def test_export_includes_blackboard(self, store, run, tmp_path):
        run(
            store.write_blackboard(
                tenant_id=TENANT,
                scope_path="graph.step1",
                content={"key": "value"},
                metadata={"source": "test"},
                created_by="agent-x",
            )
        )

        archive_dir = tmp_path / "bb-export"
        writer = BrainPortableArchiveWriter(archive_dir, vector_dim=8)
        manifest = run(writer.export(store, tenant_ids=[TENANT]))

        assert manifest.record_counts.get("blackboard", 0) >= 1
        records = list(BrainPortableArchiveReader(archive_dir).iter_records())
        bb = [r for r in records if r.type == "blackboard"]
        assert len(bb) == 1
        assert bb[0].scope_path == "graph.step1"

    def test_export_all_types(self, store, run, tmp_path):
        run(
            store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="product",
                name="Gear",
                path="product.gear",
                keywords=["outdoor"],
            )
        )
        run(store.log_trace(tenant_id=TENANT, agent_id="test-agent"))
        run(
            store.add_episode(
                id="ep-1",
                user_id="u1",
                content="Hello",
                tenant_id=TENANT,
            )
        )
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="color",
                value="blue",
            )
        )
        run(
            store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
                action_data={"tool": "search"},
                node_role="worker",
                scope_path="tenant.project",
                q_action=0.8,
                q_hypothesis=0.6,
                q_relevance=0.7,
                metadata={"latency_ms": 123, "selected_model": "test-model"},
            )
        )

        archive_dir = tmp_path / "full-export"
        writer = BrainPortableArchiveWriter(archive_dir, vector_dim=8)
        manifest = run(writer.export(store, [TENANT]))

        assert TENANT in manifest.tenants
        assert manifest.record_counts.get("taxonomy", 0) >= 1
        assert manifest.record_counts.get("trace", 0) >= 1
        assert manifest.record_counts.get("fact", 0) >= 1
        assert manifest.record_counts.get("synapse", 0) == 1

        errors = BrainPortableArchiveReader(archive_dir).validate()
        assert errors == []

    def test_idempotent_export(self, store, run, tmp_path):
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="k",
                value="v",
            )
        )
        dir1, dir2 = tmp_path / "exp1", tmp_path / "exp2"
        m1 = run(BrainPortableArchiveWriter(dir1, 8).export(store, [TENANT]))
        m2 = run(BrainPortableArchiveWriter(dir2, 8).export(store, [TENANT]))
        assert m1.record_counts == m2.record_counts


# ── Validate ──────────────────────────────────────────────────────


class TestValidate:
    def test_missing_manifest(self, tmp_path):
        errors = BrainPortableArchiveReader(tmp_path / "nope").validate()
        assert any("manifest" in e.lower() for e in errors)

    def test_corrupt_records(self, tmp_path):
        archive = tmp_path / "corrupt"
        archive.mkdir()
        (archive / "manifest.json").write_text(PortableManifest().model_dump_json())
        (archive / "records.jsonl").write_text('{"type":"fact","bad":true}\n')
        errors = BrainPortableArchiveReader(archive).validate()
        assert len(errors) > 0


# ── Import ────────────────────────────────────────────────────────


class TestImport:
    def test_dry_run_returns_counts(self, store, run, tmp_path):
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="fav",
                value="red",
            )
        )
        archive_dir = tmp_path / "dry"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        result = run(import_portable_archive(store, archive_dir, dry_run=True))
        assert result["ok"] is True
        assert result["counts"].get("fact", 0) >= 1

    def test_actual_import(self, store, run, tmp_path):
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="name",
                value="Alice",
            )
        )
        run(
            store.write_blackboard(
                tenant_id=TENANT,
                scope_path="test.bb",
                content={"imported": True},
            )
        )
        run(
            store.upsert_taxonomy(
                tenant_id=TENANT,
                domain="cat",
                name="Test",
                path="cat.test",
                keywords=["demo"],
            )
        )

        archive_dir = tmp_path / "real"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "target.sqlite3"),
            vector_dim=8,
        )
        result = run(import_portable_archive(target, archive_dir, dry_run=False))
        assert result["ok"] is True
        assert result["counts"].get("fact", 0) >= 1
        assert result["counts"].get("blackboard", 0) >= 1
        assert result["counts"].get("taxonomy", 0) >= 1

        facts = run(target.get_user_facts(user_id="u1", tenant_id=TENANT))
        assert len(facts) >= 1

    def test_tenant_remap(self, store, run, tmp_path):
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="x",
                value="y",
            )
        )
        archive_dir = tmp_path / "remap"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "remap.sqlite3"),
            vector_dim=8,
        )
        run(
            import_portable_archive(
                target,
                archive_dir,
                dry_run=False,
                tenant_map={TENANT: "remapped"},
            )
        )
        facts = run(target.get_user_facts(user_id="u1", tenant_id="remapped"))
        assert len(facts) >= 1
        # Original tenant empty
        assert run(target.get_user_facts(user_id="u1", tenant_id=TENANT)) == []

    def test_invalid_archive_raises(self, run, tmp_path):
        target = SqliteBrainStore(
            db_path=str(tmp_path / "fail.sqlite3"),
            vector_dim=8,
        )
        with pytest.raises(StorageError, match="validation failed"):
            run(
                import_portable_archive(
                    target,
                    tmp_path / "no-such",
                    dry_run=False,
                )
            )

    def test_blackboard_idempotent(self, store, run, tmp_path):
        """Re-import must NOT duplicate blackboard records."""
        result = run(
            store.write_blackboard(
                tenant_id=TENANT,
                scope_path="idem.test",
                content={"v": 1},
                ttl_seconds=3600,
                created_by="agent-x",
            )
        )
        original_id = result["id"]

        archive_dir = tmp_path / "idem"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "idem_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        records = run(
            target.read_blackboard(
                ids=[original_id],
                tenant_id=TENANT,
            )
        )
        assert len(records) == 1
        assert records[0]["id"] == original_id
        assert records[0]["content"] == {"v": 1}

    def test_trace_idempotent(self, store, run, tmp_path):
        """Re-import must NOT duplicate traces."""
        run(store.log_trace(tenant_id=TENANT, agent_id="gardener"))
        run(store.log_trace(tenant_id=TENANT, agent_id="enricher"))

        archive_dir = tmp_path / "trace-idem"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "trace_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        traces = run(target.get_traces(tenant_id=TENANT, limit=100))
        assert len(traces) == 2  # not 4

    def test_episode_idempotent(self, store, run, tmp_path):
        """Re-import must NOT duplicate episodes."""
        run(
            store.add_episode(
                id="ep-idem-1",
                user_id="u1",
                content="Hello",
                tenant_id=TENANT,
            )
        )

        archive_dir = tmp_path / "ep-idem"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "ep_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        episodes = run(
            target.get_recent_episodes(
                user_id="u1",
                tenant_id=TENANT,
                limit=100,
            )
        )
        assert len(episodes) == 1  # not 2

    def test_synapse_idempotent(self, store, run, tmp_path):
        """Re-import must NOT duplicate Synapse records."""
        original = run(
            store.record_synapse(
                tenant_id=TENANT,
                agent_id="agent-1",
                action_type="tool_call",
                action_data={"tool": "search"},
                node_role="worker",
                scope_path="tenant.project",
                q_action=0.8,
                q_hypothesis=0.6,
                q_relevance=0.7,
                metadata={"latency_ms": 123, "selected_model": "test-model"},
            )
        )
        original_id = str(original["id"])

        archive_dir = tmp_path / "synapse-idem"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "synapse_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        rows = run(target.query_synapses(tenant_id=TENANT, min_q=0.0, limit=10))
        assert len(rows) == 1
        assert rows[0]["id"] == original_id
        assert rows[0]["metadata"] == {"latency_ms": 123, "selected_model": "test-model"}


# ── Embedding validation ─────────────────────────────────────────


class TestEmbeddingValidation:
    def test_no_orphan_refs(self, store, run, tmp_path):
        run(
            store.upsert_fact(
                user_id="u1",
                tenant_id=TENANT,
                key="k",
                value="v",
            )
        )
        archive_dir = tmp_path / "emb"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))
        assert BrainPortableArchiveReader(archive_dir).validate() == []


# ── SQLite-vec -> Postgres export path ──────────────────────────────
#
# Export path test shape for SQLite-vec -> Postgres, even before full
# export lands. Everything above proves SQLite -> SQLite; this is
# the one migration direction phase-01 explicitly calls out and that had
# zero coverage before this milestone. import_portable_archive() takes any
# BrainStorageProtocol target, so no new import code was needed — only
# the test proving Postgres actually works as that target.


@pytest_asyncio.fixture
async def postgres_target():
    if not BRAIN_TEST_DSN:
        pytest.skip("BRAIN_TEST_DSN not set — skipping SQLite-vec -> Postgres export test")
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=BRAIN_TEST_DSN)
    await store.ensure_schema()
    yield store
    await store.close()


class TestSqliteToPostgresExportShape:
    @pytest.mark.asyncio
    async def test_export_from_sqlite_imports_into_postgres(
        self, store: SqliteBrainStore, tmp_path: Path, postgres_target
    ):
        tenant = f"export-shape-{uuid.uuid4().hex}"
        await store.write_blackboard(
            tenant_id=tenant,
            scope_path="export.shape.step1",
            content={"migrated": "from-sqlite"},
        )

        archive_dir = tmp_path / "sqlite-to-postgres"
        manifest = await BrainPortableArchiveWriter(archive_dir, vector_dim=8).export(
            store, tenant_ids=[tenant]
        )
        assert manifest.record_counts.get("blackboard", 0) == 1

        result = await import_portable_archive(postgres_target, archive_dir, dry_run=False)
        assert result["ok"] is True
        assert result["counts"].get("blackboard", 0) == 1

        # _import_blackboard() calls write_blackboard() (not a raw INSERT) for
        # any non-SQLite target, which mints a *new* UUID rather than
        # preserving the archived one — so the imported row must be found by
        # tenant/content, not by re-using the id from the SQLite-side export.
        pool = await postgres_target._get_pool()
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            cursor = await conn.execute(
                "SELECT content FROM blackboard WHERE tenant_id = %s",
                (tenant,),
            )
            rows = await cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == {"migrated": "from-sqlite"}
