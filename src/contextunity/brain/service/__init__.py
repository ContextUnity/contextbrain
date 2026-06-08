"""contextunity.brain gRPC Service - ContextUnit Protocol.

All methods use ContextUnit as the universal data contract.
Domain-specific data is passed via payload and validated using Pydantic models.

This module re-exports the main service classes for backward compatibility.
"""

from .brain_service import BrainService
from .embedders import ApiEmbedder, LocalEmbedder, get_embedder
from .helpers import make_response, parse_unit
from .server import serve

__all__ = [
    # Services
    "BrainService",
    # Embedders
    "ApiEmbedder",
    "LocalEmbedder",
    "get_embedder",
    # Helpers
    "parse_unit",
    "make_response",
    # Server
    "serve",
]
