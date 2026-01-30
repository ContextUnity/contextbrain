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


class PostgresKnowledgeStore(
    GraphMixin,
    EpisodesMixin,
    TaxonomyMixin,
    SearchMixin,
    PostgresStoreBase,
):
    """PostgreSQL knowledge store with pgvector and ltree support."""

    pass


__all__ = ["PostgresKnowledgeStore"]
