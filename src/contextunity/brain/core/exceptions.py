"""Exception hierarchy for contextunity.brain.

Service-specific exceptions with stable error codes for gRPC mapping.
All codes use the ``BRAIN_`` prefix so the prefix-based fallback in
``core/grpc_errors.py`` maps them to ``grpc.StatusCode.INTERNAL`` by default.

Base class and infrastructure exceptions (ConfigurationError, SecurityError, etc.)
live in ``contextunity.core.exceptions`` — import them directly from there.

Usage::

    from contextunity.brain.core.exceptions import (
        ContextbrainError,
        BrainIngestionError,
        EmbeddingError,
    )
    from contextunity.core.exceptions import StorageError
    from contextunity.core.grpc_errors import grpc_error_handler
"""

from __future__ import annotations

from contextunity.core.exceptions import ContextUnityError, register_error


@register_error("BRAIN_ERROR")
class ContextbrainError(ContextUnityError):
    """Base exception for contextunity.brain.

    Inherits from ContextUnityError so that centralized gRPC error handlers
    in contextunity.core catch brain-specific exceptions automatically.
    """

    code: str = "BRAIN_ERROR"
    message: str = "Brain service error"


@register_error("BRAIN_EMBEDDING_FAILED")
class EmbeddingError(ContextbrainError):
    """Failed to generate embeddings."""

    code: str = "BRAIN_EMBEDDING_FAILED"
    message: str = "Failed to generate embeddings"


@register_error("BRAIN_INGESTION_FAILED")
class BrainIngestionError(ContextbrainError):
    """Document ingestion failed."""

    code: str = "BRAIN_INGESTION_FAILED"
    message: str = "Document ingestion failed"


@register_error("BRAIN_STORAGE_UNAVAILABLE")
class BrainStorageError(ContextbrainError):
    """Brain storage is unavailable."""

    code: str = "BRAIN_STORAGE_UNAVAILABLE"
    message: str = "Brain storage is unavailable"


@register_error("BRAIN_PLUGIN_ERROR")
class BrainPluginError(ContextbrainError):
    """Ingestion plugin failed."""

    code: str = "BRAIN_PLUGIN_ERROR"
    message: str = "Ingestion plugin failed"


@register_error("BRAIN_GRAPH_ERROR")
class BrainGraphError(ContextbrainError):
    """Knowledge graph operation failed."""

    code: str = "BRAIN_GRAPH_ERROR"
    message: str = "Knowledge graph operation failed"


@register_error("BRAIN_REGISTRY_ERROR")
class BrainRegistryError(ContextbrainError):
    """Registry lookup or registration failed."""

    code: str = "BRAIN_REGISTRY_ERROR"
    message: str = "Registry lookup or registration failed"


@register_error("BRAIN_VALIDATION_ERROR")
class BrainValidationError(ContextbrainError):
    """Input or data validation failed."""

    code: str = "BRAIN_VALIDATION_ERROR"
    message: str = "Input or data validation failed"
