"""Exception hierarchy for contextunity.brain.textunity.brain.

Service-specific base class. All shared exceptions, ErrorRegistry, and gRPC
error handlers live in contextunity.core.exceptions — import them directly from there.

Usage:
    from contextunity.brain.core.exceptions import ContextbrainError
    from contextunity.core.exceptions import StorageError, grpc_error_handler
"""

from __future__ import annotations

from contextunity.core.exceptions import ContextUnityError


class ContextbrainError(ContextUnityError):
    """Base exception for contextunity.brain.

    Inherits from ContextUnityError so that centralized gRPC error handlers
    in contextunity.core catch brain-specific exceptions automatically.
    """

    pass
