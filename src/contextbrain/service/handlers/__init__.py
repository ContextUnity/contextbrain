"""Service handlers - modular mixins for gRPC methods."""

from .commerce import CommerceHandlersMixin
from .knowledge import KnowledgeHandlersMixin
from .memory import MemoryHandlersMixin
from .news import NewsHandlersMixin
from .taxonomy import TaxonomyHandlersMixin
from .traces import TraceHandlersMixin

__all__ = [
    "KnowledgeHandlersMixin",
    "MemoryHandlersMixin",
    "TraceHandlersMixin",
    "TaxonomyHandlersMixin",
    "NewsHandlersMixin",
    "CommerceHandlersMixin",
]
