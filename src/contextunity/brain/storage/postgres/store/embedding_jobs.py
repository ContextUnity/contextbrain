"""Durable PostgreSQL embedding job ledger and vector projection."""

from __future__ import annotations

from abc import ABC
from uuid import uuid4

from contextunity.core.sdk.payload import get_int
from contextunity.core.types import JsonDict
from psycopg import AsyncConnection

from contextunity.brain.core.exceptions import BrainValidationError

from ...embedding_jobs import (
    EmbeddingJobStatus,
    EmbeddingTransitionStatus,
    embedding_idempotency_key,
    embedding_metadata,
    embedding_transition_result,
    first_row,
)
from .base import PostgresStoreBase
from .helpers import Json, execute, fetch_all, vec


class EmbeddingJobsMixin(PostgresStoreBase, ABC):
    """PostgreSQL implementation of embedding job lifecycle operations."""

    async def enqueue_embedding_job(
        self,
        *,
        tenant_id: str,
        cell_id: str,
        content_hash: str,
        profile: str,
        max_pending: int,
    ) -> JsonDict:
        """Insert or return the idempotent job for a current cell hash."""
        async with await self.tenant_connection(tenant_id) as conn:
            cell = await fetch_all(
                conn,
                "SELECT content_hash FROM cells WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s",
                {"tenant_id": tenant_id, "cell_id": cell_id},
            )
            if not cell:
                return {"status": "rejected", "reason_code": "cell_not_found"}
            cell_row = first_row(cell)
            if cell_row is None or cell_row.get("content_hash") != content_hash:
                return {"status": "rejected", "reason_code": "content_hash_mismatch"}
            existing = await fetch_all(
                conn,
                "SELECT job_id, status, error_code, idempotency_key FROM cell_embedding_jobs "
                "WHERE tenant_id = %(tenant_id)s AND cell_id = %(cell_id)s "
                "AND content_hash = %(content_hash)s AND profile = %(profile)s",
                {
                    "tenant_id": tenant_id,
                    "cell_id": cell_id,
                    "content_hash": content_hash,
                    "profile": profile,
                },
            )
            existing_row = first_row(existing)
            if existing_row is not None:
                if (
                    existing_row.get("status") == "skipped"
                    and existing_row.get("error_code") == "content_superseded"
                ):
                    pending = await fetch_all(
                        conn,
                        "SELECT count(*) AS count FROM cell_embedding_jobs "
                        "WHERE tenant_id = %(tenant_id)s "
                        "AND status IN ('pending', 'processing')",
                        {"tenant_id": tenant_id},
                    )
                    pending_row = first_row(pending)
                    if pending_row is not None and get_int(pending_row, "count") >= max_pending:
                        return {"status": "rejected", "reason_code": "pending_limit"}
                    await execute(
                        conn,
                        "UPDATE cell_embedding_jobs SET status = 'pending', attempt = 0, "
                        "lease_id = NULL, lease_until = NULL, error_code = NULL, "
                        "updated_at = now() WHERE job_id = %(job_id)s",
                        {"job_id": existing_row["job_id"]},
                    )
                    await self._update_cell_meta(
                        conn,
                        tenant_id=tenant_id,
                        cell_id=cell_id,
                        status="pending",
                        profile=profile,
                        content_hash=content_hash,
                        attempt=0,
                    )
                    return {
                        "job_id": existing_row["job_id"],
                        "status": "pending",
                        "idempotency_key": existing_row["idempotency_key"],
                        "accepted": True,
                        "requeued": True,
                    }
                return {**existing_row, "accepted": False}
            pending = await fetch_all(
                conn,
                "SELECT count(*) AS count FROM cell_embedding_jobs WHERE tenant_id = %(tenant_id)s "
                "AND status IN ('pending', 'processing')",
                {"tenant_id": tenant_id},
            )
            pending_row = first_row(pending)
            if pending_row is not None and get_int(pending_row, "count") >= max_pending:
                return {"status": "rejected", "reason_code": "pending_limit"}
            job_id = str(uuid4())
            key = embedding_idempotency_key(
                tenant_id=tenant_id,
                cell_id=cell_id,
                content_hash=content_hash,
                profile=profile,
            )
            await execute(
                conn,
                "INSERT INTO cell_embedding_jobs "
                "(job_id, tenant_id, cell_id, content_hash, profile, status, attempt, idempotency_key) "
                "VALUES (%(job_id)s, %(tenant_id)s, %(cell_id)s, %(content_hash)s, %(profile)s, 'pending', 0, %(key)s)",
                {
                    "job_id": job_id,
                    "tenant_id": tenant_id,
                    "cell_id": cell_id,
                    "content_hash": content_hash,
                    "profile": profile,
                    "key": key,
                },
            )
            await self._update_cell_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=cell_id,
                status="pending",
                profile=profile,
                content_hash=content_hash,
                attempt=0,
            )
            return {"job_id": job_id, "status": "pending", "idempotency_key": key, "accepted": True}

    async def claim_embedding_jobs(
        self, *, tenant_id: str, limit: int, lease_seconds: int
    ) -> list[JsonDict]:
        """Lease pending jobs and reclaim expired processing jobs."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT job_id, cell_id, content_hash, profile, attempt FROM cell_embedding_jobs "
                "WHERE tenant_id = %(tenant_id)s AND (status = 'pending' OR "
                "(status = 'processing' AND lease_until < now())) "
                "ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %(limit)s",
                {"tenant_id": tenant_id, "limit": limit},
            )
            leased: list[JsonDict] = []
            for row in rows:
                lease_id = str(uuid4())
                attempt = get_int(row, "attempt") + 1
                await execute(
                    conn,
                    "UPDATE cell_embedding_jobs SET status = 'processing', attempt = %(attempt)s, "
                    "lease_id = %(lease_id)s, lease_until = now() + make_interval(secs => %(seconds)s), "
                    "updated_at = now() WHERE job_id = %(job_id)s",
                    {
                        "attempt": attempt,
                        "lease_id": lease_id,
                        "seconds": lease_seconds,
                        "job_id": row["job_id"],
                    },
                )
                await self._update_cell_meta(
                    conn,
                    tenant_id=tenant_id,
                    cell_id=str(row["cell_id"]),
                    status="processing",
                    profile=str(row["profile"]),
                    content_hash=str(row["content_hash"]),
                    attempt=attempt,
                )
                leased.append({**row, "lease_id": lease_id, "attempt": attempt})
            return leased

    async def complete_embedding_job(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
        vector: list[float],
    ) -> JsonDict:
        """Persist vector and status in one transaction for a valid lease."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = %(tenant_id)s AND job_id = %(job_id)s AND lease_id = %(lease_id)s "
                "FOR UPDATE",
                {"tenant_id": tenant_id, "job_id": job_id, "lease_id": lease_id},
            )
            row = first_row(rows)
            if row is None or row["status"] != "processing":
                return {"status": "rejected", "reason_code": "stale_lease"}
            cell = await fetch_all(
                conn,
                "SELECT content_hash FROM cells WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s",
                {"tenant_id": tenant_id, "cell_id": row["cell_id"]},
            )
            cell_row = first_row(cell)
            if cell_row is None or cell_row["content_hash"] != row["content_hash"]:
                await execute(
                    conn,
                    "UPDATE cell_embedding_jobs SET status = 'skipped', "
                    "error_code = 'content_superseded', lease_id = NULL, lease_until = NULL, "
                    "updated_at = now() WHERE job_id = %(job_id)s",
                    {"job_id": job_id},
                )
                return {"status": "skipped", "reason_code": "content_superseded"}
            await execute(
                conn,
                "UPDATE cells SET embedding = %(embedding)s::vector, updated_at = now() "
                "WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s",
                {
                    "embedding": vec(vector),
                    "tenant_id": tenant_id,
                    "cell_id": row["cell_id"],
                },
            )
            await self._update_cell_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=str(row["cell_id"]),
                status="ready",
                profile=str(row["profile"]),
                content_hash=str(row["content_hash"]),
                attempt=get_int(row, "attempt"),
            )
            await execute(
                conn,
                "UPDATE cell_embedding_jobs SET status = 'ready', lease_id = NULL, lease_until = NULL, "
                "updated_at = now() WHERE job_id = %(job_id)s",
                {"job_id": job_id},
            )
            return {"status": "ready", "attempt": row["attempt"]}

    async def restore_cell_embedding(
        self,
        *,
        tenant_id: str,
        cell_id: str,
        vector: list[float],
    ) -> None:
        """Restore one archived vector without inventing an enrichment job."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "UPDATE cells SET embedding = %(embedding)s::vector, updated_at = now() "
                "WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s RETURNING id",
                {
                    "embedding": vec(vector),
                    "tenant_id": tenant_id,
                    "cell_id": cell_id,
                },
            )
            if not rows:
                raise BrainValidationError("Cannot restore a vector for a missing BrainCell")

    async def fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Release a leased job for deterministic retry."""
        return await self._set_failure(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="pending",
        )

    async def get_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str
    ) -> JsonDict | None:
        """Return reference metadata for one valid processing lease."""
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT job_id, cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = %(tenant_id)s AND job_id = %(job_id)s AND lease_id = %(lease_id)s",
                {"tenant_id": tenant_id, "job_id": job_id, "lease_id": lease_id},
            )
            return first_row(rows)

    async def mark_embedding_skipped(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Mark a deterministic no-work status without writing a vector."""
        return await self._set_failure(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="skipped",
        )

    async def terminal_fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Mark a leased job terminally failed."""
        return await self._set_failure(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="failed",
        )

    async def _set_failure(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
        error_code: str,
        status: EmbeddingTransitionStatus,
    ) -> JsonDict:
        async with await self.tenant_connection(tenant_id) as conn:
            rows = await fetch_all(
                conn,
                "SELECT cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = %(tenant_id)s AND job_id = %(job_id)s FOR UPDATE",
                {"tenant_id": tenant_id, "job_id": job_id},
            )
            row = first_row(rows)
            if status == "failed" and row is not None and row["status"] == "failed":
                return embedding_transition_result(
                    status="failed",
                    attempt=get_int(row, "attempt"),
                    error_code=error_code,
                    idempotent=True,
                )
            lease = await fetch_all(
                conn,
                "SELECT 1 FROM cell_embedding_jobs WHERE tenant_id = %(tenant_id)s AND job_id = %(job_id)s "
                "AND lease_id = %(lease_id)s",
                {"tenant_id": tenant_id, "job_id": job_id, "lease_id": lease_id},
            )
            if row is None or row["status"] != "processing" or not lease:
                return {"status": "rejected", "reason_code": "stale_lease"}
            await self._update_cell_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=str(row["cell_id"]),
                status=status,
                profile=str(row["profile"]),
                content_hash=str(row["content_hash"]),
                attempt=get_int(row, "attempt"),
                error_code=error_code,
            )
            await execute(
                conn,
                "UPDATE cell_embedding_jobs SET status = %(status)s, error_code = %(error_code)s, "
                "lease_id = NULL, lease_until = NULL, updated_at = now() WHERE job_id = %(job_id)s",
                {"status": status, "error_code": error_code, "job_id": job_id},
            )
            return embedding_transition_result(
                status=status,
                attempt=get_int(row, "attempt"),
                error_code=error_code,
            )

    async def _update_cell_meta(
        self,
        conn: AsyncConnection[object],
        *,
        tenant_id: str,
        cell_id: str,
        status: EmbeddingJobStatus,
        profile: str,
        content_hash: str,
        attempt: int,
        error_code: str | None = None,
    ) -> None:
        patch = embedding_metadata(
            profile=profile,
            content_hash=content_hash,
            attempt=attempt,
            status=status,
            error_code=error_code,
        )
        await execute(
            conn,
            "UPDATE cells SET struct_data = (struct_data - 'embedding_error_code') "
            "|| %(patch)s::jsonb, updated_at = now() "
            "WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s",
            {"patch": Json(patch), "tenant_id": tenant_id, "cell_id": cell_id},
        )

    async def get_embedding_status(
        self, *, tenant_id: str, cell_id: str, content_hash: str | None, profile: str
    ) -> JsonDict:
        """Read job status and whether the vector column is populated."""
        async with await self.tenant_connection(tenant_id) as conn:
            where = "tenant_id = %(tenant_id)s AND cell_id = %(cell_id)s AND profile = %(profile)s"
            params: dict[str, object] = {
                "tenant_id": tenant_id,
                "cell_id": cell_id,
                "profile": profile,
            }
            if content_hash:
                where += " AND content_hash = %(content_hash)s"
                params["content_hash"] = content_hash
            rows = await fetch_all(
                conn,
                f"SELECT job_id, content_hash, status, attempt, error_code FROM cell_embedding_jobs WHERE {where} "
                "ORDER BY created_at DESC LIMIT 1",
                params,
            )
            if not rows:
                return {"status": "missing", "cell_id": cell_id, "profile": profile}
            row = first_row(rows)
            if row is None:
                return {"status": "missing", "cell_id": cell_id, "profile": profile}
            vector = await fetch_all(
                conn,
                "SELECT embedding IS NOT NULL AS present FROM cells WHERE tenant_id = %(tenant_id)s AND id = %(cell_id)s",
                {"tenant_id": tenant_id, "cell_id": cell_id},
            )
            return {
                **row,
                "cell_id": cell_id,
                "profile": profile,
                "vector_present": row.get("status") == "ready"
                and bool((first_row(vector) or {}).get("present", False)),
            }


__all__ = ["EmbeddingJobsMixin"]
