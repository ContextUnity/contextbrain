"""Core interfaces (ABCs + Protocols).
These interfaces are intentionally small and transport-agnostic.
Business logic lives in modules; orchestration lives in brain.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from contextunity.core import get_contextunit_logger
from contextunity.core.sdk.interfaces import BaseConnector, BaseTransformer
from contextunity.core.types import JsonDict

if TYPE_CHECKING:
    from contextunity.core import ContextUnit
    from contextunity.core.tokens import ContextToken

    from contextunity.brain.core.state import GraphState

logger = get_contextunit_logger(__name__)


class BaseAgent(ABC):
    """Base class for LangGraph nodes (strict: nodes are classes).

    Implementations must be async and return partial state updates.
    """

    def __init__(self, registry: object | None = None) -> None:
        """Initialize with optional component registry.

        Args:
            registry: Component registry for discovering connectors,
                transformers, providers, and models at runtime.
        """
        # Registry access (agents can discover connectors/transformers/providers/models).
        self.registry: object | None = registry

    @abstractmethod
    async def process(self, state: GraphState) -> dict[str, object]:
        """Execute agent logic on the current graph state.

        Args:
            state: Current LangGraph execution state.

        Returns:
            Partial state update dict to merge into the graph.
        """
        raise NotImplementedError

    async def __call__(self, state: GraphState) -> dict[str, object]:
        """Callable shorthand — delegates to ``process``.

        Args:
            state: Current graph execution state.

        Returns:
            Result of ``self.process(state)``.
        """
        return await self.process(state)


@runtime_checkable
class IRead(Protocol):
    """Read interface (optionally secured; enforced when security enabled)."""

    async def read(
        self,
        query: str,
        *,
        limit: int = 5,
        filters: JsonDict | None = None,
        token: ContextToken,
    ) -> list[ContextUnit]:
        """Retrieve ContextUnits matching a query.

        Args:
            query: Natural-language or structured search expression.
            limit: Maximum results to return.
            filters: Optional provider-specific filter predicates.
            token: Authentication token for access control.

        Returns:
            Ordered list of matching ContextUnits.
        """
        ...


@runtime_checkable
class IWrite(Protocol):
    """Write interface (optionally secured; enforced when security enabled)."""

    async def write(self, data: ContextUnit, *, token: ContextToken) -> None:
        """Persist a ContextUnit to the underlying store.

        Args:
            data: ContextUnit payload to persist.
            token: Authentication token for access control.
        """


class BaseProvider(ABC):
    """Sinks: accept ContextUnit envelope and persist/return it somewhere."""

    @abstractmethod
    async def sink(self, envelope: ContextUnit, *, token: ContextToken) -> object:
        """Persist a ContextUnit to the target storage.

        Args:
            envelope: ContextUnit payload to persist.
            token: Authentication token for access control.

        Returns:
            Result of the sink operation.
        """
        raise NotImplementedError


__all__ = [
    "BaseAgent",
    "BaseConnector",
    "BaseTransformer",
    "BaseProvider",
    "IRead",
    "IWrite",
]
