"""Core (vendor-agnostic) agent state models.

During migration, `contextunity.brain.cortex.state` continues to define the LangGraph
TypedDict state used in production. This module introduces Pydantic-based state
models to support stronger validation and deterministic normalization in the new
framework layout.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeAlias

from contextunity.core.narrowing import as_str
from contextunity.core.types import is_json_dict
from langchain_core.messages import BaseMessage, HumanMessage

GraphState: TypeAlias = dict[str, object]
"""Universal runtime state alias for graph execution (brain domain)."""


def _message_content_as_str(message: BaseMessage) -> str:
    content: object = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    return str(content)


def get_last_user_query(messages: Sequence[BaseMessage]) -> str:
    """Extract the text of the last HumanMessage from a list of messages."""

    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            return _message_content_as_str(msg)
        # Handle dict format if needed (sometimes used in internal state)
        if is_json_dict(msg) and msg.get("type") == "human":
            return as_str(msg.get("content", ""))
    return ""


__all__ = ["get_last_user_query", "GraphState"]
