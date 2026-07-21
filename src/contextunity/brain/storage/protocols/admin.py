"""Cross-tenant admin observability storage protocol."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from contextunity.core.types import JsonDict

from contextunity.brain.storage.protocols.cells import BrainCellStorageProtocol


@runtime_checkable
class AdminQueryProtocol(BrainCellStorageProtocol, Protocol):
    """Cross-tenant admin observability queries (Brain Admin RPC backing store).

    All methods are async so handlers can ``await`` uniformly. SQLite backends
    wrap sync SQL bodies; Postgres backends run async queries via tenant_connection.
    """

    async def list_tenants(self) -> list[JsonDict]: ...

    async def search_traces(
        self,
        *,
        tenant_id: str | None,
        agent_id: str | None,
        status: str | None = None,
        hours: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[JsonDict], int]: ...

    async def get_trace_details(self, trace_id: str) -> JsonDict | None: ...

    async def get_filter_options(self, *, tenant_id: str | None) -> JsonDict: ...

    async def get_session_traces(
        self, *, session_id: str, tenant_id: str | None
    ) -> list[JsonDict]: ...

    async def get_trace_tenant(self, trace_id: str) -> str | None: ...

    async def get_cells(
        self, *, tenant_id: str | None, kind: str | None, limit: int
    ) -> list[JsonDict]: ...

    async def get_memory_layer_stats(self, *, tenant_id: str | None) -> JsonDict: ...

    async def get_analytics_summary(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...

    async def get_system_analytics(
        self, *, tenant_id: str | None, hours: int | None
    ) -> JsonDict: ...
