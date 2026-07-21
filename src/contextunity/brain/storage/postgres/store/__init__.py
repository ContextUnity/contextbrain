"""
PostgreSQL Knowledge Store module.

Generic knowledge storage - NO business logic!
Usage: `from .store import PostgresBrainStore`
"""

from .base import PostgresStoreBase
from .blackboard import BlackboardStoreMixin
from .conversation_history import ConversationHistoryMixin
from .embedding_jobs import EmbeddingJobsMixin
from .graph import GraphMixin
from .outcomes import OutcomeObservationsMixin
from .search import SearchMixin
from .synapses import SynapsesMixin
from .trace_artifacts import TraceArtifactsMixin
from .traces import TracesMixin
from .udb import UdbMixin


class PostgresBrainStore(
    GraphMixin,
    ConversationHistoryMixin,
    TraceArtifactsMixin,
    TracesMixin,
    UdbMixin,
    SearchMixin,
    BlackboardStoreMixin,
    SynapsesMixin,
    OutcomeObservationsMixin,
    EmbeddingJobsMixin,
    PostgresStoreBase,
):
    """PostgreSQL knowledge store with pgvector and ltree support."""

    pass


__all__ = ["PostgresBrainStore"]
