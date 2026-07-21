"""Independent Brain-side bulkhead for canonical memory retrieval RPCs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from contextunity.core.concurrency import (
    BulkheadDeadlineExceededError,
    FairAsyncBulkhead,
)

from contextunity.brain.core.config import get_core_config


class BrainReadBulkhead:
    def __init__(
        self,
        *,
        enabled: bool,
        global_limit: int,
        per_tenant_limit: int,
        max_queue: int,
        per_tenant_queue_limit: int,
        deadline_ms: int,
    ) -> None:
        self.deadline_ms = deadline_ms
        self._bulkhead = FairAsyncBulkhead(
            enabled=enabled,
            global_limit=global_limit,
            per_tenant_limit=per_tenant_limit,
            max_queue=max_queue,
            per_tenant_queue_limit=per_tenant_queue_limit,
        )

    @asynccontextmanager
    async def acquire(self, tenant_id: str) -> AsyncGenerator[None, None]:
        deadline_at = time.monotonic() + self.deadline_ms / 1000.0
        async with self._bulkhead.acquire(tenant_id, deadline_at=deadline_at):
            try:
                async with asyncio.timeout_at(deadline_at):
                    yield
            except TimeoutError as exc:
                raise BulkheadDeadlineExceededError(
                    message="Brain read exceeded the server deadline"
                ) from exc


_read_bulkhead: BrainReadBulkhead | None = None


def get_brain_read_bulkhead() -> BrainReadBulkhead:
    global _read_bulkhead
    if _read_bulkhead is None:
        config = get_core_config().read_bulkhead
        _read_bulkhead = BrainReadBulkhead(
            enabled=config.enabled,
            global_limit=config.global_limit,
            per_tenant_limit=config.per_tenant_limit,
            max_queue=config.max_queue,
            per_tenant_queue_limit=config.per_tenant_queue_limit,
            deadline_ms=config.deadline_ms,
        )
    return _read_bulkhead


def reset_brain_read_bulkhead() -> None:
    global _read_bulkhead
    _read_bulkhead = None


__all__ = ["BrainReadBulkhead", "get_brain_read_bulkhead", "reset_brain_read_bulkhead"]
