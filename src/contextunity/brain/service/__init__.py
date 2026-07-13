"""contextunity.brain gRPC Service - ContextUnit Protocol.

All methods use ContextUnit as the universal data contract.
Domain-specific data is passed via payload and validated using Pydantic models.

This module exposes the main Brain service and embedding provider contracts.
"""

from .brain_service import BrainService
from .embeddings import (
    DeterministicEmbedder,
    Embedder,
    HttpEmbedder,
    OnnxEmbedder,
    SentenceTransformersEmbedder,
    get_embedder,
)
from .helpers import make_response, parse_unit
from .server import serve

__all__ = [
    # Services
    "BrainService",
    # Embedders
    "DeterministicEmbedder",
    "Embedder",
    "HttpEmbedder",
    "OnnxEmbedder",
    "SentenceTransformersEmbedder",
    "get_embedder",
    # Helpers
    "parse_unit",
    "make_response",
    # Server
    "serve",
]
