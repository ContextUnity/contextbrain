"""Modular SQLite-Vec storage backend for local Brain.

Usage::

    from contextunity.brain.storage.sqlite.store import SqliteBrainStore
"""

from .store import SqliteBrainStore

__all__ = ["SqliteBrainStore"]
