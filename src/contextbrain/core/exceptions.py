"""Exception hierarchy for contextbrain.

Service-specific base class. All shared exceptions, ErrorRegistry, and gRPC
error handlers live in contextcore.exceptions — import them directly from there.

Usage:
    from contextbrain.core.exceptions import ContextbrainError
    from contextcore.exceptions import StorageError, grpc_error_handler
"""

from __future__ import annotations

from contextcore.exceptions import ContextUnityError


class ContextbrainError(ContextUnityError):
    """Base exception for contextbrain.

    Inherits from ContextUnityError so that centralized gRPC error handlers
    in contextcore catch brain-specific exceptions automatically.
    """

    pass
