"""Canonical BrainCell persistence protocol."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from contextunity.core.types import JsonDict


class BrainCellStorageProtocol(Protocol):
    """Canonical BrainCell persistence surface shared by service and admin stores."""

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
        """Upsert a canonical BrainCell (idempotent on content_hash when supplied)."""
        ...

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
        """Query BrainCells with optional filters."""
        ...

    async def get_cell(
        self, *, tenant_id: str, cell_id: str, user_id: str | None = None
    ) -> JsonDict | None:
        """Retrieve one tenant-owned BrainCell by ID."""
        ...

    async def delete_documentation_cells(
        self,
        *,
        tenant_id: str,
        targets: Sequence[tuple[str, str]],
    ) -> JsonDict:
        """Atomically delete exact documentation cells or report a version conflict."""
        ...
