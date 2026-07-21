"""Live PostgreSQL parity for protected execution-trace artifact CAS."""

from __future__ import annotations

import os
from base64 import b64encode
from uuid import UUID, uuid4

import pytest
from contextunity.core.narrowing import as_int
from contextunity.core.sdk.execution_trace_artifacts import (
    ExecutionTraceArtifactArchiveReceipt,
    ExecutionTraceArtifactIdentity,
    ProtectedExecutionTraceArtifactEnvelope,
)
from psycopg import AsyncConnection, sql

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.storage.postgres.store import PostgresBrainStore

pytestmark = [pytest.mark.integration_live, pytest.mark.asyncio]
_DSN = (os.environ.get("BRAIN_TEST_DSN") or "").strip()


def _envelope(*, artifact_id: UUID, digest: str) -> ProtectedExecutionTraceArtifactEnvelope:
    return ProtectedExecutionTraceArtifactEnvelope(
        purpose="execution_trace_artifact/model_io",
        identity=ExecutionTraceArtifactIdentity(
            tenant_id="tenant-a",
            project_id="project-a",
            trace_id=UUID("11111111-1111-4111-8111-111111111111"),
            graph_run_id=UUID("22222222-2222-4222-8222-222222222222"),
            invocation_id=UUID("33333333-3333-4333-8333-333333333333"),
            provider_attempt_id=UUID("44444444-4444-4444-8444-444444444444"),
            artifact_kind="model_io",
        ),
        artifact_id=artifact_id,
        ciphertext_b64=b64encode(b"synthetic-ciphertext").decode(),
        content_digest=digest,
        algorithm="fernet-v1",
        key_epoch="test-epoch",
    )


async def test_postgres_artifact_reserve_finalize_read_purge_cas() -> None:
    if not _DSN:
        pytest.skip("BRAIN_TEST_DSN not set")
    schema = f"trace_artifact_{uuid4().hex}"
    store = PostgresBrainStore(
        dsn=_DSN,
        schema=schema,
        pool_min_size=1,
        pool_max_size=2,
    )
    artifact_id = uuid4()
    request = _envelope(
        artifact_id=artifact_id,
        digest="hmac-sha256:" + "a" * 64,
    )
    terminal = _envelope(
        artifact_id=artifact_id,
        digest="hmac-sha256:" + "b" * 64,
    )
    try:
        await store.ensure_schema(vector_dim=8)
        created = await store.reserve_execution_trace_artifact(
            envelope=request,
            lifecycle_profile_id="trace-artifacts-standard",
            request_bytes=5,
        )
        assert created["outcome"] == "created"
        replay = await store.reserve_execution_trace_artifact(
            envelope=request,
            lifecycle_profile_id="trace-artifacts-standard",
            request_bytes=5,
        )
        assert replay["outcome"] == "duplicate"

        finalized = await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=1,
            request_bytes=5,
            response_bytes=5,
        )
        assert finalized["revision"] == 2
        duplicate = await store.finalize_execution_trace_artifact(
            envelope=terminal,
            expected_revision=1,
            request_bytes=5,
            response_bytes=5,
        )
        assert duplicate["outcome"] == "duplicate"

        row = await store.get_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
        )
        assert row is not None
        assert row["storage_state"] == "hot"
        assert row["protected_envelope"] is not None
        assert (
            await store.get_execution_trace_artifact(
                tenant_id="tenant-a",
                project_id="project-b",
                artifact_id=str(artifact_id),
            )
            is None
        )

        begun = await store.begin_archive_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=2,
        )
        receipt = ExecutionTraceArtifactArchiveReceipt(
            artifact_id=artifact_id,
            identity=terminal.identity,
            content_digest=terminal.content_digest,
            offload_profile_id="cold-a",
            archive_generation=uuid4(),
            source_revision=2,
        )
        archived = await store.archive_execution_trace_artifact(
            receipt=receipt,
            expected_revision=as_int(begun.get("revision")),
        )
        assert archived["storage_state"] == "cold"
        with pytest.raises(BrainValidationError, match="lifecycle conflict"):
            await store.finalize_execution_trace_artifact(
                envelope=terminal,
                expected_revision=1,
                request_bytes=5,
                response_bytes=5,
            )
        restoring = await store.begin_restore_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=4,
        )
        staged = await store.stage_restore_execution_trace_artifact(
            envelope=terminal,
            expected_revision=as_int(restoring.get("revision")),
        )
        restored = await store.complete_restore_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=as_int(staged.get("revision")),
        )
        assert restored["storage_state"] == "hot"
        with pytest.raises(BrainValidationError, match="lifecycle conflict"):
            await store.finalize_execution_trace_artifact(
                envelope=terminal,
                expected_revision=1,
                request_bytes=5,
                response_bytes=5,
            )

        with pytest.raises(BrainValidationError, match="legal hold"):
            await store.begin_purge_execution_trace_artifact(
                tenant_id="tenant-a",
                project_id="project-a",
                artifact_id=str(artifact_id),
                expected_revision=7,
                legal_hold=True,
            )
        purging = await store.begin_purge_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=7,
            legal_hold=False,
        )
        purged = await store.purge_execution_trace_artifact(
            tenant_id="tenant-a",
            project_id="project-a",
            artifact_id=str(artifact_id),
            expected_revision=as_int(purging.get("revision")),
            legal_hold=False,
        )
        assert purged["storage_state"] == "purged"
        with pytest.raises(BrainValidationError, match="lifecycle conflict"):
            await store.finalize_execution_trace_artifact(
                envelope=terminal,
                expected_revision=1,
                request_bytes=5,
                response_bytes=5,
            )
    finally:
        await store.close()
        conn = await AsyncConnection.connect(_DSN, autocommit=True)
        try:
            _ = await conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
        finally:
            await conn.close()
