from __future__ import annotations

from base64 import b64encode
from pathlib import Path
from uuid import uuid4

import pytest
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ExecutionTraceArtifactIdentity,
    ProtectExecutionTraceArtifactRequest,
)
from contextunity.shield.trace_artifact_protector import TraceArtifactProtector
from cryptography.fernet import Fernet

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.sqlite import SqliteBrainStore


@pytest.fixture
def store(tmp_path: Path) -> SqliteBrainStore:
    return SqliteBrainStore(db_path=str(tmp_path / "brain.sqlite3"), vector_dim=8)


def _identity() -> ExecutionTraceArtifactIdentity:
    return ExecutionTraceArtifactIdentity(
        tenant_id="tenant-a",
        project_id="project-a",
        trace_id=uuid4(),
        graph_run_id=uuid4(),
        invocation_id=uuid4(),
        provider_attempt_id=uuid4(),
        artifact_kind="model_io",
    )


def _envelope(identity, artifact_id, content: bytes):
    protector = TraceArtifactProtector(_KEY)
    return protector.protect(
        ProtectExecutionTraceArtifactRequest(
            purpose="execution_trace_artifact/model_io",
            identity=identity,
            artifact_id=artifact_id,
            plaintext_b64=b64encode(content).decode(),
        )
    )


_KEY = Fernet.generate_key().decode()


@pytest.mark.asyncio
async def test_reserve_finalize_read_and_purge_are_identity_scoped_cas(store) -> None:
    identity = _identity()
    artifact_id = uuid4()
    request = _envelope(identity, artifact_id, b'{"request":"hello"}')
    terminal = _envelope(
        identity,
        artifact_id,
        b'{"request":"hello","response":"world"}',
    )

    created = await store.reserve_execution_trace_artifact(
        envelope=request,
        lifecycle_profile_id="trace-artifacts-standard",
        request_bytes=19,
    )
    duplicate = await store.reserve_execution_trace_artifact(
        envelope=request,
        lifecycle_profile_id="trace-artifacts-standard",
        request_bytes=19,
    )
    finalized = await store.finalize_execution_trace_artifact(
        envelope=terminal,
        expected_revision=1,
        request_bytes=19,
        response_bytes=20,
    )
    replay = await store.finalize_execution_trace_artifact(
        envelope=terminal,
        expected_revision=1,
        request_bytes=19,
        response_bytes=20,
    )
    reservation_replay = await store.reserve_execution_trace_artifact(
        envelope=request,
        lifecycle_profile_id="trace-artifacts-standard",
        request_bytes=19,
    )
    with pytest.raises(BrainValidationError, match="finalize CAS conflict"):
        await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=2,
            request_bytes=19,
            response_bytes=20,
        )
    with pytest.raises(BrainValidationError, match="finalize CAS conflict"):
        await store.finalize_execution_trace_artifact(
            envelope=request,
            expected_revision=2,
            request_bytes=19,
            response_bytes=20,
        )

    assert created["outcome"] == "created"
    assert duplicate["outcome"] == "duplicate"
    assert finalized["revision"] == 2
    assert replay["outcome"] == "duplicate"
    assert reservation_replay == {
        "artifact_id": str(artifact_id),
        "content_digest": request.content_digest,
        "revision": 2,
        "outcome": "duplicate",
    }
    assert (
        await store.get_execution_trace_artifact(
            tenant_id="tenant-b",
            project_id="project-a",
            artifact_id=str(artifact_id),
        )
        is None
    )
    record = await store.get_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
    )
    assert record is not None
    assert record["protected_envelope"]["content_digest"] == terminal.content_digest

    purging = await store.begin_purge_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=2,
        legal_hold=False,
    )
    purged = await store.purge_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=purging["revision"],
        legal_hold=False,
    )
    assert purged["storage_state"] == "purged"
    tombstone = await store.get_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
    )
    assert tombstone is not None
    assert tombstone["protected_envelope"] is None
    assert tombstone["content_digest"] == terminal.content_digest


@pytest.mark.asyncio
async def test_artifact_conflicts_and_legal_hold_fail_closed(store) -> None:
    identity = _identity()
    artifact_id = uuid4()
    first = _envelope(identity, artifact_id, b"first")
    conflicting = _envelope(identity, artifact_id, b"different")
    await store.reserve_execution_trace_artifact(
        envelope=first,
        lifecycle_profile_id="held",
        request_bytes=5,
    )

    with pytest.raises(BrainValidationError, match="conflicting"):
        await store.reserve_execution_trace_artifact(
            envelope=conflicting,
            lifecycle_profile_id="held",
            request_bytes=9,
        )
    with pytest.raises(BrainValidationError, match="legal hold"):
        await store.begin_purge_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=1,
            legal_hold=True,
        )


@pytest.mark.asyncio
async def test_archive_restore_lifecycle_uses_uri_free_receipt_and_cas(store) -> None:
    identity = _identity()
    artifact_id = uuid4()
    request = _envelope(identity, artifact_id, b"request")
    terminal = _envelope(identity, artifact_id, b"request-response")
    await store.reserve_execution_trace_artifact(
        envelope=request,
        lifecycle_profile_id="archive",
        request_bytes=7,
    )
    await store.finalize_execution_trace_artifact(
        envelope=terminal,
        expected_revision=1,
        request_bytes=7,
        response_bytes=8,
    )
    begun = await store.begin_archive_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=2,
    )
    assert begun == {
        "artifact_id": str(artifact_id),
        "storage_state": "archiving",
        "revision": 3,
    }
    receipt = ExecutionTraceArtifactArchiveReceipt(
        artifact_id=artifact_id,
        identity=identity,
        content_digest=terminal.content_digest,
        offload_profile_id="cold-a",
        archive_generation=uuid4(),
        source_revision=2,
    )
    archived = await store.archive_execution_trace_artifact(
        receipt=receipt,
        expected_revision=3,
    )
    assert archived["storage_state"] == "cold"
    cold = await store.get_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
    )
    assert cold is not None
    assert cold["protected_envelope"] is None
    assert cold["archive_receipt"]["archive_generation"] == str(receipt.archive_generation)
    with pytest.raises(BrainValidationError, match="lifecycle conflict"):
        await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=1,
            request_bytes=7,
            response_bytes=8,
        )

    restoring = await store.begin_restore_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=4,
    )
    assert restoring["storage_state"] == "restoring"
    staged = await store.stage_restore_execution_trace_artifact(
        envelope=terminal,
        expected_revision=5,
    )
    assert staged["storage_state"] == "restoring"
    restored = await store.complete_restore_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=6,
    )
    assert restored == {
        "artifact_id": str(artifact_id),
        "storage_state": "hot",
        "revision": 7,
    }
    hot = await store.get_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
    )
    assert hot is not None
    assert hot["archive_receipt"] is None
    with pytest.raises(BrainValidationError, match="lifecycle conflict"):
        await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=1,
            request_bytes=7,
            response_bytes=8,
        )
    purging = await store.begin_purge_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=7,
        legal_hold=False,
    )
    await store.purge_execution_trace_artifact(
        tenant_id="tenant-a",
        project_id="project-a",
        artifact_id=str(artifact_id),
        expected_revision=purging["revision"],
        legal_hold=False,
    )
    with pytest.raises(BrainValidationError, match="lifecycle conflict"):
        await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=1,
            request_bytes=7,
            response_bytes=8,
        )
