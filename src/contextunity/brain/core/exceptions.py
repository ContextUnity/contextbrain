"""Exception hierarchy for cu.brain.

Service-specific base class. All shared exceptions, ErrorRegistry, and gRPC
error handlers live in cu.core.exceptions — import them directly from there.

Usage:
    from contextunity.brain.core.exceptions import ContextbrainError
    from contextunity.core.exceptions import StorageError, grpc_error_handler
"""

from __future__ import annotations

from contextunity.core.exceptions import ContextUnityError


class ContextbrainError(ContextUnityError):
    """Base exception for cu.brain.

    Inherits from ContextUnityError so that centralized gRPC error handlers
    in cu.core catch brain-specific exceptions automatically.
    """

    pass
