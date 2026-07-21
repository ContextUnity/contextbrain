"""Service handlers - modular mixins for gRPC methods."""

from .admin import AdminHandlersMixin
from .blackboard import BlackboardHandlersMixin
from .cell_edges import CellEdgeHandlersMixin
from .cell_search import CellSearchHandlersMixin
from .cell_write import CellWriteHandlersMixin
from .commerce import CommerceHandlersMixin
from .embedding import EmbeddingHandlersMixin
from .memory import MemoryHandlersMixin
from .outcomes import OutcomeObservationHandlersMixin
from .synapses import SynapseHandlersMixin
from .traces import TraceHandlersMixin
from .udb import UdbHandlersMixin

__all__ = [
    "AdminHandlersMixin",
    "BlackboardHandlersMixin",
    "CellEdgeHandlersMixin",
    "CellSearchHandlersMixin",
    "CellWriteHandlersMixin",
    "MemoryHandlersMixin",
    "OutcomeObservationHandlersMixin",
    "SynapseHandlersMixin",
    "TraceHandlersMixin",
    "UdbHandlersMixin",
    "CommerceHandlersMixin",
    "EmbeddingHandlersMixin",
]
