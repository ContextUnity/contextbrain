"""SQLite implementations for Brain Admin RPCs (local ``SqliteBrainStore``)."""

from __future__ import annotations

from collections.abc import Sequence

from contextunity.core.types import JsonDict

from .admin_analytics_ops import _SqliteAnalyticsAdminOpsMixin
from .admin_memory_ops import _SqliteMemoryAdminOpsMixin
from .admin_trace_ops import _SqliteTraceAdminOpsMixin
from .store import SqliteBrainStore


class SqliteAdminOps(
    _SqliteTraceAdminOpsMixin,
    _SqliteMemoryAdminOpsMixin,
    _SqliteAnalyticsAdminOpsMixin,
):
    """Admin observability queries against the local SQLite Brain backend."""

    def __init__(self, storage: SqliteBrainStore) -> None:
        self._storage = storage


class AsyncSqliteAdminOps:
    """Async ``AdminQueryProtocol`` wrapper over sync ``SqliteAdminOps``."""

    def __init__(self, storage: SqliteBrainStore) -> None:
        self._storage = storage
        self._ops = SqliteAdminOps(storage)

    async def list_tenants(self) -> list[JsonDict]:
        return self._ops.list_tenants()

    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        status: str | None = None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]:
        return self._ops.search_traces(
            tenant_id=tenant_id,
            agent_id=agent_id,
            status=status,
            hours=hours,
            limit=limit,
            offset=offset,
        )

    async def get_trace_details(self, trace_id: str) -> JsonDict | None:
        return self._ops.get_trace_details(trace_id)

    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict:
        return self._ops.get_filter_options(tenant_id=tenant_id)

    async def get_session_traces(self, *, session_id: str, tenant_id: str | None) -> list[JsonDict]:
        return self._ops.get_session_traces(session_id=session_id, tenant_id=tenant_id)

    async def get_trace_tenant(self, trace_id: str) -> str | None:
        return self._ops.get_trace_tenant(trace_id)

    async def get_cells(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]:
        return self._ops.get_cells(tenant_id=tenant_id, kind=kind, limit=limit)

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict:
        return self._ops.get_memory_layer_stats(tenant_id=tenant_id)

    async def get_analytics_summary(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        return self._ops.get_analytics_summary(tenant_id=tenant_id, hours=hours)

    async def get_system_analytics(self, *, tenant_id: str | None, hours: int | None) -> JsonDict:
        return self._ops.get_system_analytics(tenant_id=tenant_id, hours=hours)

    # ── BrainCell canonical (Phase 3) ──────────────────────────────

    async def upsert_cell(
        self,
        *,
        tenant_id: str,
        cell_kind: str,
        content: str,
        metadata: JsonDict | None = None,
        cell_id: str | None = None,
        user_id: str | None = None,
        scope_path: str | None = None,
        content_hash: str | None = None,
        source_type: str = "manual",
        source_ref: str | None = None,
        confidence: float = 0.5,
        visibility: str = "tenant",
    ) -> JsonDict:
        return await self._storage.upsert_cell(
            tenant_id=tenant_id,
            cell_kind=cell_kind,
            content=content,
            metadata=metadata,
            cell_id=cell_id,
            user_id=user_id,
            scope_path=scope_path,
            content_hash=content_hash,
            source_type=source_type,
            source_ref=source_ref,
            confidence=confidence,
            visibility=visibility,
        )

    async def query_cells(
        self,
        *,
        tenant_id: str,
        query_text: str | None = None,
        cell_kind: str | None = None,
        source_type: str | None = None,
        scope_path: str | None = None,
        metadata_filter: JsonDict | None = None,
        limit: int = 10,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[JsonDict]:
        return await self._storage.query_cells(
            tenant_id=tenant_id,
            query_text=query_text,
            cell_kind=cell_kind,
            source_type=source_type,
            scope_path=scope_path,
            metadata_filter=metadata_filter,
            limit=limit,
            offset=offset,
            user_id=user_id,
        )

    async def get_cell(
        self, *, tenant_id: str, cell_id: str, user_id: str | None = None
    ) -> JsonDict | None:
        return await self._storage.get_cell(
            tenant_id=tenant_id,
            cell_id=cell_id,
            user_id=user_id,
        )

    async def delete_documentation_cells(
        self,
        *,
        tenant_id: str,
        targets: Sequence[tuple[str, str]],
    ) -> JsonDict:
        return await self._storage.delete_documentation_cells(
            tenant_id=tenant_id,
            targets=targets,
        )


__all__ = ["SqliteAdminOps"]
