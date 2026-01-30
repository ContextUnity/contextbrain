"""ContextBrain gRPC Service - ContextUnit Protocol.

All methods use ContextUnit as the universal data contract.
Domain-specific data is passed via payload and validated using Pydantic models.

This module re-exports the main service classes for backward compatibility.
"""

from .brain_service import BrainService
from .commerce_service import HAS_COMMERCE, CommerceService
from .embedders import LocalEmbedder, OpenAIEmbedder, get_embedder
from .helpers import make_response, parse_unit
from .server import serve

__all__ = [
    # Services
    "BrainService",
    "CommerceService",
    "HAS_COMMERCE",
    # Embedders
    "OpenAIEmbedder",
    "LocalEmbedder",
    "get_embedder",
    # Helpers
    "parse_unit",
    "make_response",
    # Server
    "serve",
]
