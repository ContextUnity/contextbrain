"""
PostgreSQL Knowledge Store module.

Generic knowledge storage - NO business logic!
Usage: `from .store import PostgresKnowledgeStore`
"""

from .base import PostgresStoreBase
from .episodes import EpisodesMixin
from .graph import GraphMixin
from .search import SearchMixin
from .taxonomy import TaxonomyMixin
from .traces import TracesMixin


class PostgresKnowledgeStore(
    GraphMixin,
    EpisodesMixin,
    TracesMixin,
    TaxonomyMixin,
    SearchMixin,
    PostgresStoreBase,
):
    """PostgreSQL knowledge store with pgvector and ltree support."""

    pass


__all__ = ["PostgresKnowledgeStore"]
