"""Plugin registry for ingestion plugins."""

from __future__ import annotations

from typing import TypeVar

from contextunity.brain.core.exceptions import BrainRegistryError

from .plugins import IngestionPlugin

_T = TypeVar("_T", bound=IngestionPlugin)

_PLUGINS: dict[str, type[IngestionPlugin]] = {}


def register_plugin(source_type: str):
    """Decorator to register a plugin class.

    Args:
        source_type: The source type string (e.g., "video", "book")

    Example:
        @register_plugin("video")
        class VideoPlugin(IngestionPlugin):
            ...
    """

    def decorator(cls: type[_T]) -> type[_T]:
        """Decorator.

        Returns:
            Type[_T]: The original class, unchanged.
        """
        _PLUGINS[source_type] = cls
        return cls

    return decorator


def get_plugin_class(source_type: str) -> type[IngestionPlugin]:
    """Retrieve a plugin class by source type.

    Args:
        source_type (str): The source type parameter.

    Returns:
        Type[IngestionPlugin]: An instance of Type[IngestionPlugin].

    Raises:
        ValueError: If parameter values are invalid.
    """
    if source_type not in _PLUGINS:
        raise BrainRegistryError(f"No plugin registered for: {source_type}")
    return _PLUGINS[source_type]


def get_all_plugins() -> list[type[IngestionPlugin]]:
    """Get all registered plugins.

    Returns:
        List of all registered plugin classes
    """
    return list(_PLUGINS.values())
