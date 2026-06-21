"""Local factory for ContextBrain gracefully degraded execution."""

import grpc
from contextunity.core import brain_pb2_grpc, get_contextunit_logger

from ..storage.duckdb_store import DuckDBStore
from ..storage.sqlite import SqliteVecStorageBackend
from .brain_service import BrainService
from .embedders import get_embedder

logger = get_contextunit_logger(__name__)


async def create_local_brain() -> grpc.aio.Server:
    """Create a gracefully degraded local Brain service."""
    logger.info("Initializing Local Brain Service (SqliteVecStorageBackend)")

    from contextunity.brain.core.config import get_core_config as get_brain_config

    brain_config = get_brain_config()

    storage = SqliteVecStorageBackend()
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

    return server


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
