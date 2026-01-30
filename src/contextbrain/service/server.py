"""Server entry point for gRPC service."""

from __future__ import annotations

import os

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
    """Start the gRPC server."""
    config = load_shared_config_from_env()
    setup_logging(config=config, service_name="contextbrain")

    server = grpc.aio.server()

    brain = BrainService()
    brain_pb2_grpc.add_BrainServiceServicer_to_server(brain, server)

    if HAS_COMMERCE:
        commerce_pb2_grpc.add_CommerceServiceServicer_to_server(CommerceService(brain), server)
        logger.info("Commerce Service registered")

    port = os.getenv("BRAIN_PORT", "50051")
    server.add_insecure_port(f"[::]:{port}")

    logger.info(f"Unified Brain Service starting on :{port} (ContextUnit Protocol)")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(serve())
