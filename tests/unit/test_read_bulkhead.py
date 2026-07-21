"""Brain-side read bulkhead and config lifecycle."""

from __future__ import annotations

import asyncio

import grpc
import pytest
from contextunity.core.concurrency import (
    BulkheadDeadlineExceededError,
    BulkheadQueueFullError,
)
from contextunity.core.grpc_errors import get_grpc_status_code
from pydantic import ValidationError

from contextunity.brain.core.config import get_core_config, reset_core_config, set_core_config
from contextunity.brain.core.config.main import BrainConfig, BrainReadBulkheadConfig
from contextunity.brain.service.read_bulkhead import (
    BrainReadBulkhead,
    get_brain_read_bulkhead,
)


@pytest.mark.asyncio
async def test_brain_read_bulkhead_rejects_saturated_tenant_as_resource_exhausted() -> None:
    bulkhead = BrainReadBulkhead(
        enabled=True,
        global_limit=1,
        per_tenant_limit=1,
        max_queue=1,
        per_tenant_queue_limit=1,
        deadline_ms=500,
    )
    release = asyncio.Event()
    entered = asyncio.Event()

    async def owner() -> None:
        async with bulkhead.acquire("tenant-a"):
            entered.set()
            await release.wait()

    first = asyncio.create_task(owner())
    await entered.wait()
    queued = asyncio.create_task(bulkhead.acquire("tenant-a").__aenter__())
    await asyncio.sleep(0)
    with pytest.raises(BulkheadQueueFullError) as captured:
        async with bulkhead.acquire("tenant-a"):
            pass
    assert get_grpc_status_code(captured.value) is grpc.StatusCode.RESOURCE_EXHAUSTED
    queued.cancel()
    release.set()
    await asyncio.gather(first, queued, return_exceptions=True)


@pytest.mark.asyncio
async def test_brain_read_bulkhead_deadline_bounds_handler_body() -> None:
    bulkhead = BrainReadBulkhead(
        enabled=True,
        global_limit=1,
        per_tenant_limit=1,
        max_queue=1,
        per_tenant_queue_limit=1,
        deadline_ms=10,
    )
    with pytest.raises(BulkheadDeadlineExceededError) as captured:
        async with bulkhead.acquire("tenant-a"):
            await asyncio.sleep(0.1)
    assert get_grpc_status_code(captured.value) is grpc.StatusCode.DEADLINE_EXCEEDED


def test_brain_read_bulkhead_env_resolves_and_defaults_on(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_core_config()
    assert get_core_config().read_bulkhead.enabled is True
    reset_core_config()
    monkeypatch.setenv("CU_BRAIN_READ_BULKHEAD_GLOBAL_LIMIT", "9")
    monkeypatch.setenv("CU_BRAIN_READ_BULKHEAD_PER_TENANT_QUEUE_LIMIT", "5")
    config = get_core_config().read_bulkhead
    assert config.global_limit == 9
    assert config.per_tenant_queue_limit == 5
    reset_core_config()


def test_brain_read_bulkhead_config_accepts_immediate_only_queue() -> None:
    config = BrainReadBulkheadConfig.model_validate({"max_queue": 0, "per_tenant_queue_limit": 0})
    assert config.max_queue == 0
    assert config.per_tenant_queue_limit == 0

    with pytest.raises(ValidationError, match="cannot exceed global queue"):
        BrainReadBulkheadConfig.model_validate({"max_queue": 0, "per_tenant_queue_limit": 1})


def test_brain_config_replacement_rebuilds_read_bulkhead_singleton() -> None:
    reset_core_config()
    first = get_brain_read_bulkhead()
    config = BrainConfig.model_validate(
        {
            "debug": True,
            "postgres": {"vector_dim": 384},
            "embeddings": {
                "provider": "deterministic",
                "space_id": "deterministic-test",
                "dimension": 384,
            },
            "read_bulkhead": {"global_limit": 7},
        }
    )
    set_core_config(config)
    second = get_brain_read_bulkhead()
    assert second is not first
    reset_core_config()
