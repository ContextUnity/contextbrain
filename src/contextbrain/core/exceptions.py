"""Exception hierarchy for contextbrain (vendor-neutral)."""

from __future__ import annotations

from typing import Any, Callable, TypeVar, cast


class ContextbrainError(Exception):
    """Base exception for contextbrain."""

    code: str = "INTERNAL_ERROR"
    message: str = "An internal error occurred"

    def __init__(self, message: str | None = None, code: str | None = None, **kwargs: Any) -> None:
        self.message = message or self.message
        self.code = code or self.code
        self.details = kwargs
        super().__init__(self.message)


class ConfigurationError(ContextbrainError):
    """Invalid or missing configuration."""

    code: str = "CONFIGURATION_ERROR"


class RetrievalError(ContextbrainError):
    """Retrieval pipeline failure."""

    code: str = "RETRIEVAL_ERROR"


class IntentDetectionError(ContextbrainError):
    """Intent classification failure."""

    code: str = "INTENT_ERROR"


class ProviderError(ContextbrainError):
    """Storage/Provider layer failure."""

    code: str = "PROVIDER_ERROR"


class SecurityError(ContextbrainError):
    """Authorization/security failure (token missing/invalid/expired)."""

    code: str = "SECURITY_ERROR"


class ConnectorError(ContextbrainError):
    """Data connector failure."""

    code: str = "CONNECTOR_ERROR"


class ModelError(ContextbrainError):
    """LLM or Embedding model failure."""

    code: str = "MODEL_ERROR"


class IngestionError(ContextbrainError):
    """Ingestion pipeline failure."""

    code: str = "INGESTION_ERROR"


class GraphBuilderError(ContextbrainError):
    """Graph building failure."""

    code: str = "GRAPH_BUILDER_ERROR"


class TransformerError(ContextbrainError):
    """Data transformation failure."""

    code: str = "TRANSFORMER_ERROR"


class StorageError(ProviderError):
    """Specific error for database or storage operations."""

    code: str = "STORAGE_ERROR"


class DatabaseConnectionError(StorageError):
    """Failed to connect to the database."""

    code: str = "DB_CONNECTION_ERROR"


# ---- Error Registry for Protocol Mapping ------------------------------------

_E = TypeVar("_E", bound=type[ContextbrainError])


class ErrorRegistry:
    """Registry for mapping internal errors to external protocol codes."""

    def __init__(self) -> None:
        self._errors: dict[str, type[ContextbrainError]] = {}

    def register(self, code: str, error_cls: type[ContextbrainError]) -> None:
        self._errors[code] = error_cls

    def get(self, code: str) -> type[ContextbrainError] | None:
        return self._errors.get(code)

    def all(self) -> dict[str, type[ContextbrainError]]:
        return dict(self._errors)


error_registry = ErrorRegistry()


def register_error(code: str) -> Callable[[_E], _E]:
    """Decorator to register a custom error type."""

    def decorator(cls: _E) -> _E:
        error_registry.register(code, cls)
        return cls

    return cast(Callable[[_E], _E], decorator)


# Register base errors
error_registry.register("INTERNAL_ERROR", ContextbrainError)
error_registry.register("CONFIGURATION_ERROR", ConfigurationError)
error_registry.register("RETRIEVAL_ERROR", RetrievalError)
error_registry.register("INTENT_ERROR", IntentDetectionError)
error_registry.register("PROVIDER_ERROR", ProviderError)
error_registry.register("SECURITY_ERROR", SecurityError)
error_registry.register("CONNECTOR_ERROR", ConnectorError)
error_registry.register("MODEL_ERROR", ModelError)
error_registry.register("INGESTION_ERROR", IngestionError)
error_registry.register("GRAPH_BUILDER_ERROR", GraphBuilderError)
error_registry.register("TRANSFORMER_ERROR", TransformerError)


# ---- gRPC Error Handling Utilities ------------------------------------------


def get_grpc_status_code(error: ContextbrainError) -> int:
    """Map ContextbrainError to gRPC status code.

    Returns grpc.StatusCode value (int) for the given error type.
    Import grpc locally to avoid dependency at module level.
    """
    import grpc

    error_to_status = {
        "CONFIGURATION_ERROR": grpc.StatusCode.FAILED_PRECONDITION,
        "SECURITY_ERROR": grpc.StatusCode.PERMISSION_DENIED,
        "RETRIEVAL_ERROR": grpc.StatusCode.NOT_FOUND,
        "PROVIDER_ERROR": grpc.StatusCode.UNAVAILABLE,
        "STORAGE_ERROR": grpc.StatusCode.UNAVAILABLE,
        "DB_CONNECTION_ERROR": grpc.StatusCode.UNAVAILABLE,
        "CONNECTOR_ERROR": grpc.StatusCode.UNAVAILABLE,
        "MODEL_ERROR": grpc.StatusCode.INTERNAL,
        "INGESTION_ERROR": grpc.StatusCode.INVALID_ARGUMENT,
        "TRANSFORMER_ERROR": grpc.StatusCode.INVALID_ARGUMENT,
    }
    return error_to_status.get(error.code, grpc.StatusCode.INTERNAL)


def grpc_error_handler(method):
    """Decorator for gRPC service methods with proper error handling.

    Catches ContextbrainError and sets appropriate gRPC status codes.
    Logs errors and ensures consistent error response format.

    Usage:
        @grpc_error_handler
        async def MyMethod(self, request, context):
            ...
    """
    import functools
    import logging

    logger = logging.getLogger(__name__)

    @functools.wraps(method)
    async def wrapper(self, request, context):
        try:
            return await method(self, request, context)
        except ContextbrainError as e:
            import grpc

            status_code = get_grpc_status_code(e)
            error_message = f"[{e.code}] {e.message}"

            logger.error(
                f"{method.__name__} failed: {error_message}",
                extra={
                    "error_code": e.code,
                    "error_details": e.details,
                },
            )

            # Set trailing metadata with error code for clients
            context.set_trailing_metadata([("error-code", e.code)])
            context.abort(status_code, error_message)

        except Exception as e:
            import grpc

            logger.exception(f"{method.__name__} unexpected error: {e}")
            context.abort(grpc.StatusCode.INTERNAL, f"Internal error: {type(e).__name__}")

    return wrapper
