"""Durable SQLite embedding job ledger and vector projection."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta

from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainValidationError

from ..embedding_jobs import (
    EmbeddingJobStatus,
    EmbeddingTransitionStatus,
    embedding_idempotency_key,
    embedding_metadata,
    embedding_transition_result,
)
from .codecs import json_dict_field, json_dumps, vec_to_bytes
from .connection import SqliteConnectionMixin


class EmbeddingJobsMixin(SqliteConnectionMixin):
    """SQLite implementation of the embedding enrichment storage contract."""

    def _embedding_meta(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        cell_id: str,
        status: EmbeddingJobStatus,
        profile: str,
        content_hash: str,
        attempt: int,
        error_code: str | None = None,
    ) -> None:
        row = conn.execute(
            "SELECT struct_data FROM cells WHERE tenant_id = ? AND id = ?",
            (tenant_id, cell_id),
        ).fetchone()
        if row is None:
            return
        metadata = dict(json_dict_field(row["struct_data"]))
        metadata.update(
            embedding_metadata(
                profile=profile,
                content_hash=content_hash,
                attempt=attempt,
                status=status,
                error_code=error_code,
            )
        )
        if error_code is None:
            metadata.pop("embedding_error_code", None)
        conn.execute(
            "UPDATE cells SET struct_data = ?, updated_at = datetime('now') "
            "WHERE tenant_id = ? AND id = ?",
            (json_dumps(metadata) or "{}", tenant_id, cell_id),
        )

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
        conn = self._get_connection()
        try:
            cell = conn.execute(
                "SELECT content_hash FROM cells WHERE tenant_id = ? AND id = ?",
                (tenant_id, cell_id),
            ).fetchone()
            if cell is None:
                return {"status": "rejected", "reason_code": "cell_not_found"}
            if cell["content_hash"] != content_hash:
                return {"status": "rejected", "reason_code": "content_hash_mismatch"}
            existing = conn.execute(
                "SELECT job_id, status, error_code, idempotency_key FROM cell_embedding_jobs "
                "WHERE tenant_id = ? AND cell_id = ? AND content_hash = ? AND profile = ?",
                (tenant_id, cell_id, content_hash, profile),
            ).fetchone()
            if existing is not None:
                if (
                    existing["status"] == "skipped"
                    and existing["error_code"] == "content_superseded"
                ):
                    pending = conn.execute(
                        "SELECT COUNT(*) AS count FROM cell_embedding_jobs WHERE tenant_id = ? "
                        "AND status IN ('pending', 'processing')",
                        (tenant_id,),
                    ).fetchone()["count"]
                    if pending >= max_pending:
                        return {"status": "rejected", "reason_code": "pending_limit"}
                    conn.execute(
                        "UPDATE cell_embedding_jobs SET status = 'pending', attempt = 0, "
                        "lease_id = NULL, lease_until = NULL, error_code = NULL, "
                        "updated_at = datetime('now') WHERE job_id = ?",
                        (existing["job_id"],),
                    )
                    self._embedding_meta(
                        conn,
                        tenant_id=tenant_id,
                        cell_id=cell_id,
                        status="pending",
                        profile=profile,
                        content_hash=content_hash,
                        attempt=0,
                    )
                    conn.commit()
                    return {
                        "job_id": existing["job_id"],
                        "status": "pending",
                        "idempotency_key": existing["idempotency_key"],
                        "accepted": True,
                        "requeued": True,
                    }
                return {
                    "job_id": existing["job_id"],
                    "status": existing["status"],
                    "idempotency_key": existing["idempotency_key"],
                    "accepted": False,
                }
            pending = conn.execute(
                "SELECT COUNT(*) AS count FROM cell_embedding_jobs WHERE tenant_id = ? "
                "AND status IN ('pending', 'processing')",
                (tenant_id,),
            ).fetchone()["count"]
            if pending >= max_pending:
                return {"status": "rejected", "reason_code": "pending_limit"}
            job_id = str(uuid.uuid4())
            key = embedding_idempotency_key(
                tenant_id=tenant_id,
                cell_id=cell_id,
                content_hash=content_hash,
                profile=profile,
            )
            conn.execute(
                "INSERT INTO cell_embedding_jobs "
                "(job_id, tenant_id, cell_id, content_hash, profile, status, attempt, idempotency_key) "
                "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)",
                (job_id, tenant_id, cell_id, content_hash, profile, key),
            )
            self._embedding_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=cell_id,
                status="pending",
                profile=profile,
                content_hash=content_hash,
                attempt=0,
            )
            conn.commit()
            return {
                "job_id": job_id,
                "status": "pending",
                "idempotency_key": key,
                "accepted": True,
            }
        finally:
            conn.close()

    async def claim_embedding_jobs(
        self, *, tenant_id: str, limit: int, lease_seconds: int
    ) -> list[JsonDict]:
        """Lease a bounded batch, reclaiming expired processing jobs."""
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = datetime.now(UTC)
            rows = conn.execute(
                "SELECT job_id, cell_id, content_hash, profile, attempt FROM cell_embedding_jobs "
                "WHERE tenant_id = ? AND (status = 'pending' OR "
                "(status = 'processing' AND lease_until < ?)) "
                "ORDER BY created_at LIMIT ?",
                (tenant_id, now.isoformat(), limit),
            ).fetchall()
            leased: list[JsonDict] = []
            for row in rows:
                lease_id = str(uuid.uuid4())
                attempt = int(row["attempt"]) + 1
                lease_until = now + timedelta(seconds=lease_seconds)
                conn.execute(
                    "UPDATE cell_embedding_jobs SET status = 'processing', attempt = ?, "
                    "lease_id = ?, lease_until = ?, updated_at = datetime('now') WHERE job_id = ?",
                    (attempt, lease_id, lease_until.isoformat(), row["job_id"]),
                )
                self._embedding_meta(
                    conn,
                    tenant_id=tenant_id,
                    cell_id=row["cell_id"],
                    status="processing",
                    profile=row["profile"],
                    content_hash=row["content_hash"],
                    attempt=attempt,
                )
                leased.append(
                    {
                        "job_id": row["job_id"],
                        "cell_id": row["cell_id"],
                        "content_hash": row["content_hash"],
                        "profile": row["profile"],
                        "lease_id": lease_id,
                        "attempt": attempt,
                    }
                )
            conn.commit()
            return leased
        finally:
            conn.close()

    async def complete_embedding_job(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
        vector: list[float],
    ) -> JsonDict:
        """Persist a vector and mark a valid lease ready atomically."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = ? AND job_id = ? AND lease_id = ?",
                (tenant_id, job_id, lease_id),
            ).fetchone()
            if row is None or row["status"] != "processing":
                return {"status": "rejected", "reason_code": "stale_lease"}
            cell = conn.execute(
                "SELECT content_hash FROM cells WHERE tenant_id = ? AND id = ?",
                (tenant_id, row["cell_id"]),
            ).fetchone()
            if cell is None or cell["content_hash"] != row["content_hash"]:
                conn.execute(
                    "UPDATE cell_embedding_jobs SET status = 'skipped', error_code = ?, "
                    "lease_id = NULL, lease_until = NULL, updated_at = datetime('now') "
                    "WHERE job_id = ?",
                    ("content_superseded", job_id),
                )
                conn.commit()
                return {"status": "skipped", "reason_code": "content_superseded"}
            if not self.has_sqlite_vec():
                self._embedding_meta(
                    conn,
                    tenant_id=tenant_id,
                    cell_id=row["cell_id"],
                    status="skipped",
                    profile=row["profile"],
                    content_hash=row["content_hash"],
                    attempt=row["attempt"],
                    error_code="sqlite_vec_unavailable",
                )
                conn.execute(
                    "UPDATE cell_embedding_jobs SET status = 'skipped', error_code = ?, "
                    "lease_id = NULL, lease_until = NULL, updated_at = datetime('now') WHERE job_id = ?",
                    ("sqlite_vec_unavailable", job_id),
                )
                conn.commit()
                return {"status": "skipped", "reason_code": "sqlite_vec_unavailable"}
            # sqlite-vec virtual tables do not support ON CONFLICT UPSERT.
            conn.execute("DELETE FROM vec_cells WHERE node_id = ?", (row["cell_id"],))
            conn.execute(
                "INSERT INTO vec_cells (node_id, embedding) VALUES (?, ?)",
                (row["cell_id"], vec_to_bytes(vector)),
            )
            self._embedding_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=row["cell_id"],
                status="ready",
                profile=row["profile"],
                content_hash=row["content_hash"],
                attempt=row["attempt"],
            )
            conn.execute(
                "UPDATE cell_embedding_jobs SET status = 'ready', lease_id = NULL, "
                "lease_until = NULL, updated_at = datetime('now') WHERE job_id = ?",
                (job_id,),
            )
            conn.commit()
            return {"status": "ready", "attempt": row["attempt"]}
        finally:
            conn.close()

    async def restore_cell_embedding(
        self,
        *,
        tenant_id: str,
        cell_id: str,
        vector: list[float],
    ) -> None:
        """Restore one archived vector without inventing an enrichment job."""
        if len(vector) != self.vector_dim:
            raise BrainValidationError(
                f"Archived vector dimension {len(vector)} does not match {self.vector_dim}",
            )
        if not self.has_sqlite_vec():
            raise BrainValidationError(
                "sqlite-vec is required to restore archived BrainCell vectors",
            )
        conn = self._get_connection()
        try:
            cell = conn.execute(
                "SELECT 1 FROM cells WHERE tenant_id = ? AND id = ?",
                (tenant_id, cell_id),
            ).fetchone()
            if cell is None:
                raise BrainValidationError("Cannot restore a vector for a missing BrainCell")
            conn.execute("DELETE FROM vec_cells WHERE node_id = ?", (cell_id,))
            conn.execute(
                "INSERT INTO vec_cells (node_id, embedding) VALUES (?, ?)",
                (cell_id, vec_to_bytes(vector)),
            )
            conn.commit()
        finally:
            conn.close()

    async def get_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str
    ) -> JsonDict | None:
        """Return reference metadata for one valid processing lease."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT job_id, cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = ? AND job_id = ? AND lease_id = ?",
                (tenant_id, job_id, lease_id),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    async def mark_embedding_skipped(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Mark deterministic no-work status without touching a vector."""
        return self._transition_leased_job(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="skipped",
        )

    async def fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Release a lease for retry; caller decides terminal failure."""
        return self._transition_leased_job(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="pending",
        )

    async def terminal_fail_embedding_job(
        self, *, tenant_id: str, job_id: str, lease_id: str, error_code: str
    ) -> JsonDict:
        """Mark a leased job failed idempotently."""
        return self._transition_leased_job(
            tenant_id=tenant_id,
            job_id=job_id,
            lease_id=lease_id,
            error_code=error_code,
            status="failed",
        )

    def _transition_leased_job(
        self,
        *,
        tenant_id: str,
        job_id: str,
        lease_id: str,
        error_code: str,
        status: EmbeddingTransitionStatus,
    ) -> JsonDict:
        """Apply one terminal or retryable transition after validating a lease."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT cell_id, content_hash, profile, attempt, status FROM cell_embedding_jobs "
                "WHERE tenant_id = ? AND job_id = ?",
                (tenant_id, job_id),
            ).fetchone()
            if status == "failed" and row is not None and row["status"] == "failed":
                return embedding_transition_result(
                    status="failed",
                    attempt=int(row["attempt"]),
                    error_code=error_code,
                    idempotent=True,
                )
            if (
                row is None
                or row["status"] != "processing"
                or not conn.execute(
                    "SELECT 1 FROM cell_embedding_jobs "
                    "WHERE tenant_id = ? AND job_id = ? AND lease_id = ?",
                    (tenant_id, job_id, lease_id),
                ).fetchone()
            ):
                return {"status": "rejected", "reason_code": "stale_lease"}
            self._embedding_meta(
                conn,
                tenant_id=tenant_id,
                cell_id=row["cell_id"],
                status=status,
                profile=row["profile"],
                content_hash=row["content_hash"],
                attempt=row["attempt"],
                error_code=error_code,
            )
            conn.execute(
                "UPDATE cell_embedding_jobs SET status = ?, error_code = ?, "
                "lease_id = NULL, lease_until = NULL, updated_at = datetime('now') WHERE job_id = ?",
                (status, error_code, job_id),
            )
            conn.commit()
            return embedding_transition_result(
                status=status,
                attempt=int(row["attempt"]),
                error_code=error_code,
            )
        finally:
            conn.close()

    async def get_embedding_status(
        self, *, tenant_id: str, cell_id: str, content_hash: str | None, profile: str
    ) -> JsonDict:
        """Read status without returning cell content or vector values."""
        conn = self._get_connection()
        try:
            query = (
                "SELECT job_id, content_hash, status, attempt, error_code FROM cell_embedding_jobs "
            )
            query += "WHERE tenant_id = ? AND cell_id = ? AND profile = ?"
            params: list[object] = [tenant_id, cell_id, profile]
            if content_hash:
                query += " AND content_hash = ?"
                params.append(content_hash)
            query += " ORDER BY created_at DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
            if row is None:
                return {"status": "missing", "cell_id": cell_id, "profile": profile}
            return {
                "job_id": row["job_id"],
                "cell_id": cell_id,
                "content_hash": row["content_hash"],
                "status": row["status"],
                "attempt": row["attempt"],
                "error_code": row["error_code"],
                "profile": profile,
                "vector_present": row["status"] == "ready" and self._vector_present(conn, cell_id),
            }
        finally:
            conn.close()

    def _vector_present(self, conn: sqlite3.Connection, cell_id: str) -> bool:
        if not self.has_sqlite_vec():
            return False
        row = conn.execute("SELECT node_id FROM vec_cells WHERE node_id = ?", (cell_id,)).fetchone()
        return row is not None


__all__ = ["EmbeddingJobsMixin"]
