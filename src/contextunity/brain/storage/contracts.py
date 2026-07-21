"""Canonical storage protocol for Brain backends.

Defines the handler-facing contract that both PostgresBrainStore and
SqliteBrainStore must satisfy. Derived from actual ``self.storage.*``
calls in ``services/brain/src/contextunity/brain/service/handlers/``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from contextunity.brain.storage.protocols.admin import AdminQueryProtocol
from contextunity.brain.storage.protocols.cells import BrainCellStorageProtocol
from contextunity.brain.storage.protocols.connection import TenantConnection
from contextunity.brain.storage.protocols.knowledge import KnowledgeStorageProtocol
from contextunity.brain.storage.protocols.lifecycle import LifecycleStorageProtocol
from contextunity.brain.storage.protocols.memory import MemoryStorageProtocol
from contextunity.brain.storage.protocols.udb import UdbStorageProtocol


@runtime_checkable
class BrainStorageProtocol(
    BrainCellStorageProtocol,
    LifecycleStorageProtocol,
    KnowledgeStorageProtocol,
    MemoryStorageProtocol,
    UdbStorageProtocol,
    Protocol,
):
    """Minimum storage interface consumed by Brain gRPC handlers.

    Handler mixins add their consumed methods here as their RPC verticals land.
    Backends that do not support an operation must raise a typed error
    (e.g. ``UnsupportedLocalModeError``), never return silent empty success.
    """


__all__ = [
    "AdminQueryProtocol",
    "BrainStorageProtocol",
    "TenantConnection",
    "UdbStorageProtocol",
]
