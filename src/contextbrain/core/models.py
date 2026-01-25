"""Core data models for ContextBrain.

This module defines Pydantic models used throughout the brain for retrieval,
citations, and state management.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RetrievedDoc(BaseModel):
    """A retrieved document with metadata and content.

    This model represents a document retrieved from a knowledge store,
    including its content, metadata, and relevance score.
    """

    source_type: str = Field(..., description="Type of source (book, video, qa, web, etc.)")
    content: str = Field(..., description="Main content of the document")
    title: str | None = Field(None, description="Title of the document")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    relevance: float | None = Field(0.0, description="Relevance score from retrieval")

    # Optional fields for different source types
    url: str | None = None
    snippet: str | None = None
    keywords: list[str] | None = None
    summary: str | None = None
    quote: str | None = None

    # Book-specific fields
    book_title: str | None = None
    chapter: str | None = None
    chapter_number: int | None = None
    page_start: int | None = None
    page_end: int | None = None

    # Video-specific fields
    video_id: str | None = None
    video_url: str | None = None
    video_name: str | None = None
    timestamp: str | None = None
    timestamp_seconds: float | None = None

    # QA-specific fields
    session_title: str | None = None
    question: str | None = None
    answer: str | None = None

    # File-specific fields
    filename: str | None = None
    description: str | None = None


class Citation(BaseModel):
    """A citation reference for retrieved content.

    This model represents a citation that can be displayed in UI,
    linking back to the source document.
    """

    source_type: str = Field(..., description="Type of source")
    title: str | None = Field(None, description="Title of the cited document")
    content: str = Field(..., description="Cited content excerpt")
    url: str | None = Field(None, description="URL to the source")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional citation metadata"
    )
