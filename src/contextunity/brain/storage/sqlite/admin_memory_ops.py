"""SQLite Conversation History and cell statistics for Brain Admin."""

from __future__ import annotations

from contextunity.core.narrowing import as_int
from contextunity.core.types import JsonDict

from ..embedding_jobs import embedding_job_status_counts
from .store import SqliteBrainStore
from .traces import sqlite_row_to_json_dict


class _SqliteMemoryAdminOpsMixin:
    _storage: SqliteBrainStore

    def get_cells(
        self,
        *,
        tenant_id: str | None,
        kind: str | None,
        limit: int,
    ) -> list[JsonDict]:
        conditions: list[str] = []
        params: list[object] = []
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if kind:
            conditions.append("cell_kind = ?")
            params.append(kind)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self._storage.get_sqlite_connection() as db:
            rows = db.execute(
                f"""
                SELECT id, cell_kind, source_type, title,
                       SUBSTR(content, 1, 200) AS content_preview,
                       tenant_id, created_at
                FROM cells {where}
                ORDER BY created_at DESC LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
        return [
            {
                "id": str(data.get("id") or ""),
                "cell_kind": str(data.get("cell_kind") or ""),
                "source_type": str(data.get("source_type") or ""),
                "title": str(data.get("title") or ""),
                "content_preview": str(data.get("content_preview") or ""),
                "tenant_id": str(data.get("tenant_id") or ""),
                "created_at": str(data.get("created_at") or ""),
            }
            for row in rows
            if (data := sqlite_row_to_json_dict(row))
        ]

    def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict:
        where = "WHERE tenant_id = ?" if tenant_id else ""
        params: list[object] = [tenant_id] if tenant_id else []
        with self._storage.get_sqlite_connection() as db:
            conversation_row = db.execute(
                f"SELECT COUNT(*) AS total FROM conversation_records {where}", params
            ).fetchone()
            cells_row = db.execute(
                f"SELECT COUNT(*) AS total FROM cells {where}", params
            ).fetchone()
            source_rows = db.execute(
                f"SELECT source_type, COUNT(*) AS total FROM cells {where} "
                "GROUP BY source_type ORDER BY source_type",
                params,
            ).fetchall()
            job_rows = db.execute(
                f"SELECT status, COUNT(*) AS count FROM cell_embedding_jobs {where} "
                "GROUP BY status ORDER BY status",
                params,
            ).fetchall()
        conversation_count = (
            as_int(sqlite_row_to_json_dict(conversation_row).get("total"))
            if conversation_row
            else 0
        )
        cells_count = as_int(sqlite_row_to_json_dict(cells_row).get("total")) if cells_row else 0
        source_types: JsonDict = {
            str(data.get("source_type") or "unknown"): as_int(data.get("total"))
            for row in source_rows
            if (data := sqlite_row_to_json_dict(row))
        }
        return {
            "conversation_records": {"count": conversation_count},
            "cells": {"count": cells_count, "by_source_type": source_types},
            "embedding_jobs": embedding_job_status_counts(
                sqlite_row_to_json_dict(row) for row in job_rows
            ),
        }


__all__ = ["_SqliteMemoryAdminOpsMixin"]
