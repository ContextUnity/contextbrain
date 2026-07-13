"""Service handlers - modular mixins for gRPC methods."""

from .admin import AdminHandlersMixin
from .blackboard import BlackboardHandlersMixin
from .commerce import CommerceHandlersMixin
from .embedding import EmbeddingHandlersMixin
from .knowledge import KnowledgeHandlersMixin
from .memory import MemoryHandlersMixin
from .synapses import SynapseHandlersMixin
from .taxonomy import TaxonomyHandlersMixin
from .traces import TraceHandlersMixin

__all__ = [
    "AdminHandlersMixin",
    "BlackboardHandlersMixin",
    "KnowledgeHandlersMixin",
    "MemoryHandlersMixin",
    "SynapseHandlersMixin",
    "TraceHandlersMixin",
    "TaxonomyHandlersMixin",
    "CommerceHandlersMixin",
    "EmbeddingHandlersMixin",
]
