"""Module providing Module docstring is missing capabilities."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import duckdb
from contextunity.core.narrowing import as_float, as_str
from pydantic import BaseModel, Field

__all__ = ["ReflectionEntry", "Journal"]


class ReflectionEntry(BaseModel):
    """
    Data model for a single unit of agent reflection.

    Attributes:
        entry_id: Unique UUID for the reflection.
        agent_id: Identifier of the agent (e.g., 'Overlord', 'Matcher').
        context_hash: Deterministic hash of the input context/prompt.
        decision_summary: Brief description of the agent's decision.
        feedback: Human or automated feedback text.
        score: Numerical score (-1.0 to 1.0) indicating quality.
        created_at: Timestamp of creation.
    """

    entry_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str
    context_hash: str  # Fingerprint of the input
    decision_summary: str
    feedback: str  # Human comment
    score: float  # -1.0 to 1.0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def _row_created_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return datetime.now(timezone.utc)


class Journal:
    """
    Persistent storage for agent reflections using DuckDB.

    Acts as a long-term memory for feedback loops, allowing agents
    to query past mistakes or successes.
    """

    con: duckdb.DuckDBPyConnection

    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize the Journal database.

        Args:
            db_path (str): Path to DuckDB file. Defaults to in-memory.
        """
        self.con = duckdb.connect(db_path)
        _ = self.con.execute("""
            CREATE TABLE IF NOT EXISTS reflections (
                entry_id TEXT PRIMARY KEY,
                agent_id TEXT,
                context_hash TEXT,
                decision_summary TEXT,
                feedback TEXT,
                score DOUBLE,
                created_at TIMESTAMP
            )
        """)

    def add_entry(self, entry: ReflectionEntry) -> None:
        """
        Record a new reflection entry.

        Args:
            entry (ReflectionEntry): The reflection object to persist.
        """
        _ = self.con.execute(
            """
            INSERT INTO reflections
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                entry.entry_id,
                entry.agent_id,
                entry.context_hash,
                entry.decision_summary,
                entry.feedback,
                entry.score,
                entry.created_at,
            ),
        )

    def get_reflections(self, agent_id: str, limit: int = 5) -> list[ReflectionEntry]:
        """
        Retrieve recent reflections for a specific agent.

        Args:
            agent_id (str): The agent identifier to filter by.
            limit (int): Max number of entries to return.

        Returns:
            list[ReflectionEntry]: List of found reflections, ordered by time.
        """
        res = self.con.execute(
            """
            SELECT * FROM reflections
            WHERE agent_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """,
            (agent_id, limit),
        ).fetchall()

        entries: list[ReflectionEntry] = []
        for raw_row in res:
            if len(raw_row) < 7:
                continue
            entry_id: object = raw_row[0]
            row_agent_id: object = raw_row[1]
            context_hash: object = raw_row[2]
            decision_summary: object = raw_row[3]
            feedback: object = raw_row[4]
            score: object = raw_row[5]
            created_at_raw: object = raw_row[6]
            entries.append(
                ReflectionEntry(
                    entry_id=as_str(entry_id),
                    agent_id=as_str(row_agent_id),
                    context_hash=as_str(context_hash),
                    decision_summary=as_str(decision_summary),
                    feedback=as_str(feedback),
                    score=as_float(score),
                    created_at=_row_created_at(created_at_raw),
                )
            )
        return entries
