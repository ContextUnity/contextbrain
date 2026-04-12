import uuid
from datetime import datetime, timezone
from typing import List

import duckdb
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


class Journal:
    """
    Persistent storage for agent reflections using DuckDB.

    Acts as a long-term memory for feedback loops, allowing agents
    to query past mistakes or successes.
    """

    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize the Journal database.

        Args:
            db_path (str): Path to DuckDB file. Defaults to in-memory.
        """
        self.con = duckdb.connect(db_path)
        self.con.execute("""
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
        self.con.execute(
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

    def get_reflections(self, agent_id: str, limit: int = 5) -> List[ReflectionEntry]:
        """
        Retrieve recent reflections for a specific agent.

        Args:
            agent_id (str): The agent identifier to filter by.
            limit (int): Max number of entries to return.

        Returns:
            List[ReflectionEntry]: List of found reflections, ordered by time.
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

        entries = []
        for r in res:
            entries.append(
                ReflectionEntry(
                    entry_id=r[0],
                    agent_id=r[1],
                    context_hash=r[2],
                    decision_summary=r[3],
                    feedback=r[4],
                    score=r[5],
                    created_at=r[6],
                )
            )
        return entries
