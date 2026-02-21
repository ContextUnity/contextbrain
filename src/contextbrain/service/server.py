"""Server entry point for gRPC service."""

from __future__ import annotations

import asyncio
import signal

import grpc
from contextcore import (
    brain_pb2_grpc,
    get_context_unit_logger,
    load_shared_config_from_env,
    register_service,
    setup_logging,
)
from contextcore.security import get_security_interceptors, shield_status

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

    interceptors = list(get_security_interceptors())
    interceptors.append(BrainPermissionInterceptor())

    server = grpc.aio.server(interceptors=interceptors)

    # Log security status
    sec = shield_status()
    sec_log = logger.info if sec["security_enabled"] else logger.warning
    sec_log(
        "Security: enabled=%s, shield=%s",
        sec["security_enabled"],
        "active" if sec["shield_active"] else "not installed",
    )

    brain = BrainService()

    # Ensure schema exists on startup (idempotent — uses IF NOT EXISTS)
    include_commerce = HAS_COMMERCE
    await brain.storage.ensure_schema(
        include_commerce=include_commerce,
        include_news_engine=brain_config.news_engine,
        vector_dim=brain_config.postgres.vector_dim,
    )

    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)

    if HAS_COMMERCE:
        commerce_pb2_grpc.add_CommerceServiceServicer_to_server(CommerceService(brain), server)
        logger.info("Commerce Service registered")

    port = brain_config.port
    instance_name = brain_config.instance_name

    from contextcore.grpc_utils import create_server_credentials

    tls_creds = create_server_credentials()
    if tls_creds:
        server.add_secure_port(f"[::]:{port}", tls_creds)
        logger.info("Brain Service starting on :%s with TLS (instance=%s)", port, instance_name)
    else:
        server.add_insecure_port(f"[::]:{port}")
        logger.info("Brain Service starting on :%s (instance=%s)", port, instance_name)
    await server.start()

    # Register in Redis for service discovery (ContextView, other services)
    # BRAIN_TENANTS: comma-separated tenant IDs this Brain serves.
    #   Empty = shared instance (serves all tenants).
    #   Example: BRAIN_TENANTS=nszu → only nszu project discovers this Brain.
    tenants = brain_config.tenants

    heartbeat_task = await register_service(
        service="brain",
        instance=instance_name,
        endpoint=f"localhost:{port}",
        tenants=tenants,
        metadata={"port": port},
    )

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown_handler():
        logger.info("Shutdown signal received, stopping Brain...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown_handler)

    await stop_event.wait()
    logger.info("Stopping gRPC server (5s grace)...")
    await server.stop(grace=5)
    if heartbeat_task:
        heartbeat_task.cancel()
    logger.info("Brain server stopped.")


if __name__ == "__main__":
    # .env loaded in serve() via Config.load()
    asyncio.run(serve())
