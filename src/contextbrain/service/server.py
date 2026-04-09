"""Server entry point for gRPC service."""

from __future__ import annotations

import asyncio

import grpc
from contextcore import (
    brain_pb2_grpc,
    get_context_unit_logger,
    load_shared_config_from_env,
    setup_logging,
)

from .brain_service import BrainService
from .commerce_service import HAS_COMMERCE

if HAS_COMMERCE:
    from contextcore import commerce_pb2_grpc

    from .commerce_service import CommerceService

logger = get_context_unit_logger(__name__)


async def serve():
    """Start the gRPC server. Config: .env loaded only via Config.load() (single entry)."""
    from contextbrain.core.config import Config

    Config.load()  # Load service .env once; port and rest from env
    config = load_shared_config_from_env()
    setup_logging(config=config, service_name="contextbrain")

    # Load Brain-specific config for security settings
    from contextbrain.core import get_core_config

    brain_config = get_core_config()

    # Build interceptor list: security + domain permission checks
    from .interceptors import BrainPermissionInterceptor

    interceptors = []
    interceptors.append(BrainPermissionInterceptor(shield_url=config.shield_url))

    server = grpc.aio.server(
        interceptors=interceptors,
        options=(
            ("grpc.so_reuseport", 1 if config.grpc_reuse_port else 0),
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ),
    )

    brain = BrainService()

    # Ensure schema exists on startup (idempotent — uses IF NOT EXISTS)
    include_commerce = HAS_COMMERCE
    await brain.storage.ensure_schema(
        include_commerce=include_commerce,
        vector_dim=brain_config.postgres.vector_dim,
    )

    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)

    if HAS_COMMERCE:
        commerce_pb2_grpc.add_CommerceServiceServicer_to_server(CommerceService(brain), server)
        logger.info("Commerce Service registered")

    from contextcore.grpc_utils import graceful_shutdown, start_grpc_server

    heartbeat_task = await start_grpc_server(
        server,
        "brain",
        brain_config.port,
        instance_name=brain_config.instance_name,
        tenants=brain_config.tenants,
    )

    await graceful_shutdown(server, "Brain", heartbeat_task=heartbeat_task)


if __name__ == "__main__":
    # .env loaded in serve() via Config.load()
    asyncio.run(serve())
