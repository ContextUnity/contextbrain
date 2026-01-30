"""Service handlers - modular mixins for gRPC methods."""

from .commerce import CommerceHandlersMixin
from .knowledge import KnowledgeHandlersMixin
from .memory import MemoryHandlersMixin
from .news import NewsHandlersMixin
from .taxonomy import TaxonomyHandlersMixin

__all__ = [
    "KnowledgeHandlersMixin",
    "MemoryHandlersMixin",
    "TaxonomyHandlersMixin",
    "NewsHandlersMixin",
    "CommerceHandlersMixin",
]
