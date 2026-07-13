"""Local factory for ContextBrain gracefully degraded execution."""

from __future__ import annotations

import asyncio
import contextlib
from importlib.util import find_spec

import grpc
from contextunity.core import brain_pb2_grpc, get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError

from ..storage.duckdb_store import DuckDBStore
from ..storage.sqlite import SqliteBrainStore
from .brain_service import BrainService
from .embeddings import get_embedder

logger = get_contextunit_logger(__name__)


def _validate_local_vector_support(
    *, enrichment_enabled: bool, provider: str, has_sqlite_vec: bool
) -> None:
    """Fail before local startup when enabled enrichment lacks its vector runtime."""
    if not enrichment_enabled:
        return
    missing: list[str] = []
    if not has_sqlite_vec:
        missing.append("sqlite-vec")
    provider_modules = {
        "onnx": ("onnxruntime", "huggingface_hub", "numpy", "tokenizers"),
        "sentence_transformers": ("sentence_transformers",),
    }
    for module in provider_modules.get(provider, ()):
        if find_spec(module) is None:
            missing.append(module)
    if missing:
        raise ConfigurationError(
            "Local embedding enrichment requires vector support that is not installed: "
            + ", ".join(missing)
            + ". Install with: uv sync --package contextunity-cli --extra local-vectors"
        )


class LocalBrainServer:
    """gRPC server wrapper with local Blackboard maintenance lifecycle."""

    def __init__(
        self,
        *,
        grpc_server: grpc.aio.Server,
        storage: SqliteBrainStore,
        prune_interval_seconds: float,
    ) -> None:
        self.grpc_server = grpc_server
        self._storage = storage
        self._prune_interval_seconds = prune_interval_seconds
        self._prune_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self.grpc_server.start()
        if self._prune_interval_seconds > 0:
            self._prune_task = asyncio.create_task(
                self._blackboard_prune_loop(),
                name="local-brain-blackboard-prune",
            )

    async def stop(self, grace: float | None = None) -> None:
        if self._prune_task is not None:
            self._prune_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._prune_task
            self._prune_task = None
        await self.grpc_server.stop(grace)

    async def wait_for_termination(self, timeout: float | None = None) -> bool:
        return await self.grpc_server.wait_for_termination(timeout)

    async def _blackboard_prune_loop(self) -> None:
        while True:
            await asyncio.sleep(self._prune_interval_seconds)
            try:
                deleted = await self._storage.prune_expired_blackboard()
            except Exception:
                logger.exception("Local Brain Blackboard prune failed")
                continue
            if deleted:
                logger.info("Local Brain Blackboard prune: deleted=%d", deleted)


async def create_local_brain() -> LocalBrainServer:
    """Create a gracefully degraded local Brain service."""
    logger.info("Initializing Local Brain Service (SqliteBrainStore)")

    from contextunity.brain.core.config import get_core_config as get_brain_config

    brain_config = get_brain_config()

    storage = SqliteBrainStore(vector_dim=brain_config.embeddings.dimension)
    _validate_local_vector_support(
        enrichment_enabled=brain_config.embedding_enrichment.enabled,
        provider=brain_config.embeddings.provider,
        has_sqlite_vec=storage.has_sqlite_vec(),
    )
    brain = BrainService(
        storage=storage,
        duckdb=DuckDBStore(),
        embedder=get_embedder(brain_config),
    )

    from .interceptors import BrainPermissionInterceptor

    shield_url = brain_config.shield_url
    logger.info("Local Brain: shield_url=%s", shield_url or "(disabled)")

    server = grpc.aio.server(
        interceptors=[BrainPermissionInterceptor(shield_url=shield_url, config=brain_config)]
    )
    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)
    _ = server.add_insecure_port(f"[::]:{brain_config.port}")

    return LocalBrainServer(
        grpc_server=server,
        storage=storage,
        prune_interval_seconds=getattr(brain_config, "blackboard_prune_interval_seconds", 300.0),
    )


if __name__ == "__main__":
    import asyncio

    from contextunity.core.logging import setup_logging

    setup_logging()

    async def _run() -> None:
        server = await create_local_brain()
        _ = await server.start()
        print("Brain gRPC listening (local)")
        _ = await server.wait_for_termination()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
