"""Server entry point for gRPC service."""

from __future__ import annotations

import asyncio

import grpc
from contextunity.core import (
    brain_pb2_grpc,
    get_contextunit_logger,
    setup_logging,
)

from .brain_service import BrainService

logger = get_contextunit_logger(__name__)


async def serve():
    """Start the gRPC server."""
    from contextunity.brain.core.config import get_core_config

    brain_config = get_core_config()  # Load service config once
    setup_logging(config=brain_config, service_name="contextunity.brain")

    # Build interceptor list: security + domain permission checks
    from .interceptors import BrainPermissionInterceptor

    interceptors: list[grpc.aio.ServerInterceptor] = [
        BrainPermissionInterceptor(
            shield_url=brain_config.shield_url,
            config=brain_config,
        ),
    ]

    server = grpc.aio.server(
        interceptors=interceptors,
        options=(
            ("grpc.so_reuseport", 1 if brain_config.grpc_reuse_port else 0),
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ),
    )

    brain = BrainService()

    # Ensure schema exists on startup (idempotent — uses IF NOT EXISTS)
    await brain.storage.ensure_schema(vector_dim=brain_config.postgres.vector_dim)

    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)

    from contextunity.core.grpc_utils import graceful_shutdown, start_grpc_server

    runtime_handle = await start_grpc_server(
        server,
        "brain",
        brain_config.port,
        host=brain_config.host,
        instance_name=brain_config.instance_name,
        tenants=brain_config.tenants,
        redis_url=brain_config.redis.url,
        config=brain_config,
    )

    await graceful_shutdown(server, "Brain", runtime_handle=runtime_handle)


if __name__ == "__main__":
    # .env loaded in serve() via get_core_config()
    asyncio.run(serve())
