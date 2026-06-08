"""Tests for Portable Archive v1 — export, validation, import, embeddings.

Run: ``uv run pytest services/brain/tests/unit/test_portable_archive.py -v``
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from contextunity.core.exceptions import StorageError

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.portable import (
    BrainPortableArchiveReader,
    BrainPortableArchiveWriter,
    FactRecord,
    PortableManifest,
    import_portable_archive,
    parse_record,
)
from contextunity.brain.storage.sqlite import SqliteVecStorageBackend


@pytest.fixture
def store(tmp_path: Path) -> SqliteVecStorageBackend:
    return SqliteVecStorageBackend(
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

        archive_dir = tmp_path / "full-export"
        writer = BrainPortableArchiveWriter(archive_dir, vector_dim=8)
        manifest = run(writer.export(store, [TENANT]))

        assert TENANT in manifest.tenants
        assert manifest.record_counts.get("taxonomy", 0) >= 1
        assert manifest.record_counts.get("trace", 0) >= 1
        assert manifest.record_counts.get("fact", 0) >= 1

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

        target = SqliteVecStorageBackend(
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

        target = SqliteVecStorageBackend(
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
        target = SqliteVecStorageBackend(
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

        target = SqliteVecStorageBackend(
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

        target = SqliteVecStorageBackend(
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

        target = SqliteVecStorageBackend(
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
