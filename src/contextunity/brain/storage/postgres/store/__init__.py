"""
PostgreSQL Knowledge Store module.

Generic knowledge storage - NO business logic!
Usage: `from .store import PostgresBrainStore`
"""

from .base import PostgresStoreBase
from .blackboard import BlackboardStoreMixin
from .embedding_jobs import EmbeddingJobsMixin
from .episodes import EpisodesMixin
from .graph import GraphMixin
from .search import SearchMixin
from .synapses import SynapsesMixin
from .taxonomy import TaxonomyMixin
from .traces import TracesMixin


class PostgresBrainStore(
    GraphMixin,
    EpisodesMixin,
    TracesMixin,
    TaxonomyMixin,
    SearchMixin,
    BlackboardStoreMixin,
    SynapsesMixin,
    EmbeddingJobsMixin,
    PostgresStoreBase,
):
    """PostgreSQL knowledge store with pgvector and ltree support."""

    pass


__all__ = ["PostgresBrainStore"]
