"""Tests for Portable Archive export, validation, import, and embeddings.

Run: ``uv run pytest services/brain/tests/unit/test_portable_archive.py -v``
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from hashlib import sha256
from pathlib import Path

import pytest
import pytest_asyncio
from contextunity.core.exceptions import StorageError
from contextunity.core.sdk.conversation import (
    conversation_content_hash,
    conversation_record_id,
    conversation_source_hash,
)
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.portable import (
    BrainPortableArchiveReader,
    BrainPortableArchiveWriter,
    CellRecord,
    PortableManifest,
    SynapseRecord,
    TraceRecord,
    import_portable_archive,
    parse_record,
)
from contextunity.brain.storage.portable.importer import _import_trace
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


def _terminal_trace() -> JsonDict:
    trace: JsonDict = {
        "schema_version": "contextunity.execution-trace/v2",
        "trace_id": str(uuid.uuid4()),
        "graph_run_id": str(uuid.uuid4()),
        "tenant_id": TENANT,
        "agent_id": "router-agent",
        "project_id": "project-a",
        "graph_name": "graph-a",
        "terminal_status": "succeeded",
        "terminal_reason": "verified_success",
        "duration_ms": 0,
        "steps": [],
        "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
        "prompt_evidence": [],
        "provenance": [],
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
        json.dumps(trace, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return trace


class TestParseRecord:
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

    def test_v3_trace_rejects_raw_guidance_in_open_step_data(self):
        with pytest.raises(ValueError, match="Extra inputs"):
            parse_record(
                json.dumps(
                    {
                        "type": "trace",
                        "tenant_id": "t",
                        "trace_id": "11111111-1111-4111-8111-111111111111",
                        "agent_id": "router",
                        "trace_schema_version": "contextunity.execution-trace/v3",
                        "steps": [
                            {
                                "sequence": 0,
                                "attempt_id": "33333333-3333-4333-8333-333333333333",
                                "kind": "model",
                                "name": "test/model",
                                "status": "succeeded",
                                "duration_ms": 0,
                                "usage": {
                                    "input_tokens": 0,
                                    "output_tokens": 0,
                                    "cost_micros": 0,
                                },
                                "guidance_evidence": {
                                    "origin": "graph_llm_node",
                                    "purpose": "agentic_reasoning",
                                    "mode": "required",
                                    "outcome": "applied_once",
                                    "policy_version": "v1",
                                    "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
                                    "descriptor": {
                                        "artifact_id": "core.agentic-ethos",
                                        "artifact_version": "v1",
                                        "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                                        "release_id": "2026.07.1",
                                    },
                                    "content": "must never enter a portable archive",
                                },
                            }
                        ],
                        "created_at": "2026-07-18T00:00:00+00:00",
                    }
                )
            )

    def test_current_format_rejects_legacy_cell_kind(self):
        with pytest.raises(ValueError, match="node_kind"):
            parse_record(
                json.dumps(
                    {
                        "type": "cell",
                        "tenant_id": "t",
                        "id": "cell-1",
                        "content": "invalid current record",
                        "node_kind": "fact",
                    }
                )
            )


# ── Export ────────────────────────────────────────────────────────


class TestExport:
    def test_export_excludes_fixture_tenant(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id="_test",
                cell_kind="fact",
                content="synthetic fixture",
            )
        )

        manifest = run(
            BrainPortableArchiveWriter(tmp_path / "fixture-export", 8).export(store, ["_test"])
        )

        assert "_test" not in manifest.tenants
        assert manifest.record_counts == {}

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
        run(store.log_trace(tenant_id=TENANT, agent_id="test-agent"))
        run(
            store.append_conversation_record(
                record_id=conversation_record_id(
                    tenant_id=TENANT, idempotency_key="portable:record-1"
                ),
                tenant_id=TENANT,
                user_id="u1",
                content="Hello",
                session_id=None,
                role="user",
                kind="message",
                content_hash=conversation_content_hash("Hello"),
                source_hash=conversation_source_hash("portable:test:record-1"),
                graph_run_id=None,
                metadata_version=1,
                idempotency_key="portable:record-1",
                metadata={},
            )
        )
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="color=blue",
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
        assert manifest.record_counts.get("trace", 0) >= 1
        assert manifest.record_counts.get("conversation", 0) == 1
        assert manifest.record_counts.get("cell", 0) >= 1
        assert manifest.record_counts.get("synapse", 0) == 1

        errors = BrainPortableArchiveReader(archive_dir).validate()
        assert errors == []

    def test_idempotent_export(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="k=v",
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
    def test_v5_portable_record_requires_explicit_root_control_evidence(self) -> None:
        with pytest.raises(ValueError, match="requires root control evidence"):
            TraceRecord(
                tenant_id=TENANT,
                trace_id="11111111-1111-4111-8111-111111111111",
                agent_id="router-agent",
                graph_run_id="22222222-2222-4222-8222-222222222222",
                payload_digest="a" * 64,
                terminal_status="failed",
                terminal_reason="failed",
                trace_schema_version="contextunity.execution-trace/v5",
                created_at="2026-07-06T00:00:00+00:00",
            )

    def test_dry_run_returns_counts(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="fav=red",
            )
        )
        archive_dir = tmp_path / "dry"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        result = run(import_portable_archive(store, archive_dir, dry_run=True))
        assert result["ok"] is True
        assert result["counts"].get("cell", 0) >= 1

    def test_actual_import(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="name=Alice",
                source_type="auto_extract",
                source_ref="trace-1",
                scope_path="acme.memory",
                confidence=0.7,
                visibility="user",
            )
        )
        run(
            store.write_blackboard(
                tenant_id=TENANT,
                scope_path="test.bb",
                content={"imported": True},
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
        assert result["counts"].get("cell", 0) >= 1
        assert result["counts"].get("blackboard", 0) >= 1

        # legacy get_user_facts removed; query via cells instead
        cells = run(target.query_cells(tenant_id=TENANT, cell_kind="fact", limit=10))
        assert len(cells) >= 1
        imported = next(cell for cell in cells if cell["content"] == "name=Alice")
        assert imported["source_type"] == "auto_extract"
        assert imported["source_ref"] == "trace-1"
        assert imported["scope_path"] == "acme.memory"
        assert imported["confidence"] == 0.7
        assert imported["visibility"] == "user"

    def test_tenant_remap(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="x=y",
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
        cells = run(target.query_cells(tenant_id="remapped", cell_kind="fact", limit=5))
        assert len(cells) >= 1
        # Original tenant empty
        assert run(target.query_cells(tenant_id=TENANT, cell_kind="fact", limit=5)) == []

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

    def test_terminal_trace_round_trip_preserves_receipt_identity(self, store, run, tmp_path):
        terminal = _terminal_trace()
        receipt = run(store.finalize_execution_trace(terminal_trace=terminal))
        archive_dir = tmp_path / "terminal-trace"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "terminal_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        traces = run(target.get_traces(tenant_id=TENANT, limit=100))
        assert len(traces) == 1
        assert traces[0]["id"] == receipt["trace_id"]
        assert traces[0]["graph_run_id"] == receipt["graph_run_id"]
        assert traces[0]["payload_digest"] == receipt["digest"]
        assert traces[0]["trace_schema_version"] == "contextunity.execution-trace/v2"
        assert traces[0]["control_evidence"] == terminal["control_evidence"]

    def test_v3_guidance_evidence_survives_portable_round_trip(self, store, run, tmp_path):
        terminal = _terminal_trace()
        terminal["schema_version"] = "contextunity.execution-trace/v3"
        terminal["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "55555555-5555-4555-8555-555555555555",
                "kind": "model",
                "name": "test/model",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 3,
                    "cost_micros": 2,
                    "provider_details": {
                        "schema_id": "openai.responses.usage/v1",
                        "values": {
                            "openai.cached_input_tokens": 7,
                            "openai.reasoning_output_tokens": 2,
                        },
                    },
                },
                "guidance_evidence": {
                    "origin": "graph_llm_node",
                    "purpose": "agentic_reasoning",
                    "mode": "required",
                    "outcome": "applied_once",
                    "policy_version": "v1",
                    "policy_digest": "b1c8f3995fae62701ab5a955083d6ba7b211d7a6f371cf4e67c061e3580a6e8b",
                    "descriptor": {
                        "artifact_id": "core.agentic-ethos",
                        "artifact_version": "v1",
                        "content_digest": "176ed2a85316a932a2f88a90d2f987e5d2855aeb2038379a74dbbcabbd563cd1",
                        "release_id": "2026.07.1",
                    },
                },
            }
        ]
        terminal["usage"] = {"input_tokens": 10, "output_tokens": 3, "cost_micros": 2}
        control = terminal["control_evidence"]
        assert isinstance(control, dict)
        control["model_attempts"] = 1
        terminal.pop("digest", None)
        terminal["digest"] = sha256(
            json.dumps(
                terminal,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        receipt = run(store.finalize_execution_trace(terminal_trace=terminal))
        archive_dir = tmp_path / "terminal-trace-v3"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "terminal_target_v3.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))

        traces = run(target.get_traces(tenant_id=TENANT, limit=100))
        assert traces[0]["id"] == receipt["trace_id"]
        assert traces[0]["trace_schema_version"] == "contextunity.execution-trace/v3"
        steps = traces[0]["steps"]
        assert isinstance(steps, list)
        assert steps[0]["guidance_evidence"]["outcome"] == "applied_once"
        assert "content" not in steps[0]["guidance_evidence"]
        assert steps[0]["usage"]["provider_details"] == {
            "schema_id": "openai.responses.usage/v1",
            "values": {
                "openai.cached_input_tokens": 7,
                "openai.reasoning_output_tokens": 2,
            },
        }

    def test_v5_control_step_survives_portable_round_trip(self, store, run, tmp_path):
        terminal = _terminal_trace()
        terminal["schema_version"] = "contextunity.execution-trace/v5"
        terminal["steps"] = [
            {
                "sequence": 0,
                "attempt_id": "55555555-5555-4555-8555-555555555555",
                "kind": "node",
                "name": "worker",
                "status": "succeeded",
                "duration_ms": 1,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
            },
            {
                "sequence": 1,
                "attempt_id": "55555555-5555-4555-8555-555555555556",
                "kind": "control",
                "name": "router_censor",
                "status": "succeeded",
                "duration_ms": 0,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                "control_action": "request_replan",
                "control_reason": "stagnation_low_q_repeated_fault",
                "evidence_refs": ["policy:" + "a" * 64, "verifier:node:verifier"],
                "replan_request": {
                    "run_id": terminal["graph_run_id"],
                    "reason": "stagnation_low_q_repeated_fault",
                    "verifier_ref": "node:verifier",
                    "policy_digest": "a" * 64,
                    "failed_task_ids": [],
                    "stalled_task_ids": ["worker"],
                    "remaining_provider_attempts": 1,
                    "remaining_node_attempts": 1,
                    "remaining_graph_cycles": 1,
                    "remaining_side_effect_attempts": 1,
                    "remaining_input_tokens": 1,
                    "remaining_output_tokens": 1,
                    "remaining_cost_micros": 1,
                    "remaining_wall_time_ms": 1,
                    "fault_refs": [],
                    "effect_receipt_refs": [],
                    "progress_hashes": [],
                    "stagnation_hashes": ["b" * 64],
                },
            },
        ]
        terminal["usage"] = {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0}
        terminal["terminal_status"] = "failed"
        terminal["terminal_reason"] = "replan_requested"
        terminal.pop("digest", None)
        terminal["digest"] = sha256(
            json.dumps(
                terminal,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        receipt = run(store.finalize_execution_trace(terminal_trace=terminal))
        archive_dir = tmp_path / "terminal-trace-v5"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "terminal_target_v5.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))

        traces = run(target.get_traces(tenant_id=TENANT, limit=100))
        assert traces[0]["id"] == receipt["trace_id"]
        assert traces[0]["trace_schema_version"] == "contextunity.execution-trace/v5"
        steps = traces[0]["steps"]
        assert isinstance(steps, list)
        assert steps[1]["kind"] == "control"
        assert steps[1]["control_action"] == "request_replan"
        assert "payload" not in steps[1]

    @pytest.mark.asyncio
    async def test_v5_import_validates_complete_terminal_before_generic_store_write(self) -> None:
        fault_id = "77777777-7777-4777-8777-777777777777"
        record = TraceRecord(
            tenant_id=TENANT,
            trace_id="11111111-1111-4111-8111-111111111111",
            agent_id="router-agent",
            graph_name="graph-a",
            metadata={"project_id": "project-a"},
            graph_run_id="22222222-2222-4222-8222-222222222222",
            payload_digest="a" * 64,
            terminal_status="failed",
            terminal_reason="replan_requested",
            trace_schema_version="contextunity.execution-trace/v5",
            token_usage={"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
            timing_ms=1,
            steps=[
                {
                    "sequence": 0,
                    "attempt_id": "33333333-3333-4333-8333-333333333333",
                    "kind": "control",
                    "name": "router_censor",
                    "status": "succeeded",
                    "duration_ms": 0,
                    "usage": {"input_tokens": 0, "output_tokens": 0, "cost_micros": 0},
                    "control_action": "request_replan",
                    "control_reason": "stagnation_detected",
                    "evidence_refs": [
                        "policy:" + "a" * 64,
                        "verifier:node:verifier",
                        f"fault:{fault_id}",
                    ],
                    "replan_request": {
                        "run_id": "22222222-2222-4222-8222-222222222222",
                        "reason": "stagnation_detected",
                        "verifier_ref": "node:verifier",
                        "policy_digest": "a" * 64,
                        "failed_task_ids": [],
                        "stalled_task_ids": [],
                        "remaining_provider_attempts": 1,
                        "remaining_node_attempts": 1,
                        "remaining_graph_cycles": 1,
                        "remaining_side_effect_attempts": 1,
                        "remaining_input_tokens": 1,
                        "remaining_output_tokens": 1,
                        "remaining_cost_micros": 1,
                        "remaining_wall_time_ms": 1,
                        "fault_refs": [fault_id],
                        "effect_receipt_refs": [],
                        "progress_hashes": [],
                        "stagnation_hashes": [],
                    },
                }
            ],
            control_evidence={
                "node_attempts": 0,
                "failed_node_attempts": 0,
                "model_attempts": 0,
                "failed_model_attempts": 0,
                "tool_attempts": 0,
                "failed_tool_attempts": 0,
                "graph_cycles": 0,
                "contribution_refs": [],
                "invalid_contribution_refs": [],
                "fault_refs": [],
                "effect_receipt_refs": [],
                "effect_receipts": [],
            },
            created_at="2026-07-06T00:00:00+00:00",
        )

        class _NoWriteStore:
            writes = 0

            async def finalize_execution_trace(self, *, terminal_trace: JsonDict) -> JsonDict:
                _ = terminal_trace
                self.writes += 1
                return {}

            async def log_trace(
                self,
                *,
                tenant_id: str,
                agent_id: str,
                session_id: str | None = None,
                user_id: str | None = None,
                graph_name: str | None = None,
                tool_calls: list[JsonDict] | None = None,
                token_usage: JsonDict | None = None,
                timing_ms: int | None = None,
                security_flags: JsonDict | None = None,
                metadata: JsonDict | None = None,
                provenance: list[str] | None = None,
            ) -> str:
                _ = (
                    tenant_id,
                    agent_id,
                    session_id,
                    user_id,
                    graph_name,
                    tool_calls,
                    token_usage,
                    timing_ms,
                    security_flags,
                    metadata,
                    provenance,
                )
                self.writes += 1
                return "unexpected"

        store = _NoWriteStore()
        with pytest.raises(ValueError, match="control evidence"):
            await _import_trace(store, record, TENANT)
        assert store.writes == 0

    def test_conversation_history_idempotent(self, store, run, tmp_path):
        """Re-import must return the durable duplicate without adding rows."""
        idempotency_key = "portable:record-idem-1"
        record_id = conversation_record_id(tenant_id=TENANT, idempotency_key=idempotency_key)
        run(
            store.append_conversation_record(
                record_id=record_id,
                tenant_id=TENANT,
                user_id="u1",
                content="Hello",
                session_id=None,
                role="user",
                kind="message",
                content_hash=conversation_content_hash("Hello"),
                source_hash=conversation_source_hash("portable:test:record-idem-1"),
                graph_run_id=None,
                metadata_version=1,
                idempotency_key=idempotency_key,
                metadata={},
            )
        )

        archive_dir = tmp_path / "conversation-idem"
        run(BrainPortableArchiveWriter(archive_dir, 8).export(store, [TENANT]))

        target = SqliteBrainStore(
            db_path=str(tmp_path / "conversation_target.sqlite3"),
            vector_dim=8,
        )
        run(import_portable_archive(target, archive_dir, dry_run=False))
        run(import_portable_archive(target, archive_dir, dry_run=False))

        records = run(
            target.query_conversation_history(
                user_id="u1",
                tenant_id=TENANT,
                projection="recent",
                session_id=None,
                graph_run_id=None,
                older_than_days=None,
                limit=100,
                offset=0,
            )
        )
        assert [record.record_id for record in records] == [record_id]

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
    def test_referenced_embedding_file_is_required(self, tmp_path):
        archive_dir = tmp_path / "missing-embeddings"
        archive_dir.mkdir()
        (archive_dir / "manifest.json").write_text(
            PortableManifest(vector_dim=8).model_dump_json(),
        )
        (archive_dir / "records.jsonl").write_text(
            CellRecord(
                tenant_id=TENANT,
                id="cell-1",
                content="has a vector reference",
                cell_kind="fact",
                content_hash="sha256:cell-1",
                confidence=0.5,
                created_at="2026-07-13T00:00:00+00:00",
                updated_at="2026-07-13T00:00:00+00:00",
                embedding_ref="emb:cell:cell-1",
            ).model_dump_json()
            + "\n",
        )

        errors = BrainPortableArchiveReader(archive_dir).validate()

        assert errors == ["Missing embeddings.jsonl for referenced vectors"]

    def test_no_orphan_refs(self, store, run, tmp_path):
        run(
            store.upsert_cell(
                tenant_id=TENANT,
                user_id="u1",
                cell_kind="fact",
                content="k=v",
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
    from psycopg import AsyncConnection, sql

    from contextunity.brain.storage.postgres import PostgresBrainStore

    schema = f"portable_trace_{uuid.uuid4().hex}"
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
        conversation_key = "portable:sqlite-postgres:turn-1"
        conversation_id = conversation_record_id(tenant_id=tenant, idempotency_key=conversation_key)
        graph_run_id = uuid.uuid4()
        await store.append_conversation_record(
            record_id=conversation_id,
            tenant_id=tenant,
            user_id="user-a",
            session_id="session-a",
            role="assistant",
            kind="conversation_note",
            content="portable conversation",
            content_hash=conversation_content_hash("portable conversation"),
            source_hash=conversation_source_hash("portable:sqlite-postgres:source"),
            graph_run_id=graph_run_id,
            metadata_version=1,
            idempotency_key=conversation_key,
            metadata={"origin": "sqlite"},
        )

        archive_dir = tmp_path / "sqlite-to-postgres"
        manifest = await BrainPortableArchiveWriter(archive_dir, vector_dim=8).export(
            store, tenant_ids=[tenant]
        )
        assert manifest.record_counts.get("blackboard", 0) == 1
        assert manifest.record_counts.get("conversation", 0) == 1

        result = await import_portable_archive(postgres_target, archive_dir, dry_run=False)
        assert result["ok"] is True
        assert result["counts"].get("blackboard", 0) == 1
        assert result["counts"].get("conversation", 0) == 1

        conversations = await postgres_target.query_conversation_history(
            tenant_id=tenant,
            projection="recent",
            user_id="user-a",
            session_id=None,
            graph_run_id=None,
            older_than_days=None,
            limit=10,
            offset=0,
        )
        assert len(conversations) == 1
        assert conversations[0].record_id == conversation_id
        assert conversations[0].graph_run_id == graph_run_id
        assert conversations[0].idempotency_key == conversation_key
        assert conversations[0].metadata == {"origin": "sqlite"}

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
