"""Simplified registry system with factory pattern.
Design goals:
- **Minimal abstraction** - only essential registries remain
- **Factory pattern** for core components (providers, connectors)
- **Direct imports** for static components where possible
- **Backward compatibility** - existing code continues to work
"""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar

from contextunity.core import get_contextunit_logger

from contextunity.brain.core.exceptions import BrainRegistryError

# ---- Graph Registry -------------------------------------------------

# Graph registry defined later in file, forward reference for now
graph_registry: "Registry"


class GraphBuilder(Protocol):
    """Factory object that builds and returns a graph instance."""

    def __call__(self) -> object: ...


C = TypeVar("C", bound=type)


def register_graph(name: str) -> Callable[[GraphBuilder], GraphBuilder]:
    """Decorator to register a graph builder.

    Args:
        name: Graph name/key for lookup in config

    Returns:
        Decorator function

    Example:
        @register_graph("my_project_graph")
        def build_my_graph():
            # Inline graph building logic
            return StateGraph(...)
    """

    def decorator(func: GraphBuilder) -> GraphBuilder:
        graph_registry.register(name, func)
        return func

    return decorator


# ---- Factory Classes ------------------------------------------------


@dataclass(frozen=True)
class ComponentConstructor:
    """Runtime class constructor wrapper with an explicit object return type."""

    target: type[object]

    def __call__(self, **kwargs: object) -> object:
        instance: object = self.target(**kwargs)
        return instance


def _constructor_for(target: type[object]) -> ComponentConstructor:
    return ComponentConstructor(target)


def _module_symbol(module_name: str, symbol_name: str) -> object:
    module = importlib.import_module(module_name)
    exports: dict[str, object] = dict(vars(module))
    if symbol_name not in exports:
        raise BrainRegistryError(f"{module_name} has no symbol {symbol_name!r}")
    return exports[symbol_name]


def _load_constructor(module_name: str, class_name: str) -> ComponentConstructor:
    candidate = _module_symbol(module_name, class_name)
    if not isinstance(candidate, type):
        raise BrainRegistryError(f"{module_name}.{class_name} is not a class constructor")
    return _constructor_for(candidate)


class ComponentFactory:
    """Factory for creating core components."""

    # Dynamic factories populated by decorators
    _provider_factories: dict[str, ComponentConstructor] = {}
    _connector_factories: dict[str, ComponentConstructor] = {}
    _transformer_factories: dict[str, ComponentConstructor] = {}

    @classmethod
    def register_provider_factory(cls, name: str, factory: ComponentConstructor) -> None:
        cls._provider_factories[name] = factory

    @classmethod
    def register_connector_factory(cls, name: str, factory: ComponentConstructor) -> None:
        cls._connector_factories[name] = factory

    @classmethod
    def register_transformer_factory(cls, name: str, factory: ComponentConstructor) -> None:
        cls._transformer_factories[name] = factory

    @staticmethod
    def create_provider(name: str, **kwargs: object) -> object:
        """Create a storage provider instance."""
        if name in ComponentFactory._provider_factories:
            return ComponentFactory._provider_factories[name](**kwargs)

        # Fallback to built-in providers
        providers = {
            "postgres": (
                "contextunity.brain.modules.providers.storage.postgres.provider",
                "PostgresProvider",
            ),
            "gcs": ("contextunity.brain.modules.providers.storage.gcs", "GCSProvider"),
        }

        if name not in providers:
            raise BrainRegistryError(f"Unknown provider: {name}")

        module_name, class_name = providers[name]
        constructor = _load_constructor(module_name, class_name)
        return constructor(**kwargs)

    @staticmethod
    def create_connector(name: str, **kwargs: object) -> object:
        """Create a data connector instance."""
        if name in ComponentFactory._connector_factories:
            return ComponentFactory._connector_factories[name](**kwargs)

        # Fallback to built-in connectors
        connectors = {
            "web": ("contextunity.brain.modules.connectors.web", "WebSearchConnector"),
            "web_scraper": (
                "contextunity.brain.modules.connectors.web",
                "WebScraperConnector",
            ),
            "file": ("contextunity.brain.modules.connectors.file", "FileConnector"),
            "rss": ("contextunity.brain.modules.connectors.rss", "RSSConnector"),
            "api": ("contextunity.brain.modules.connectors.api", "APIConnector"),
        }

        if name not in connectors:
            raise BrainRegistryError(f"Unknown connector: {name}")

        module_name, class_name = connectors[name]
        constructor = _load_constructor(module_name, class_name)
        return constructor(**kwargs)

    @staticmethod
    def create_transformer(name: str, **kwargs: object) -> object:
        """Create a transformer instance."""
        if name in ComponentFactory._transformer_factories:
            return ComponentFactory._transformer_factories[name](**kwargs)

        # Fallback to built-in transformers
        transformers = {
            "metadata_mapper": (
                "contextunity.brain.modules.transformers.metadata",
                "MetadataMapper",
            ),
            "summarizer": (
                "contextunity.brain.modules.transformers.summarization",
                "Summarizer",
            ),
        }

        if name not in transformers:
            raise BrainRegistryError(f"Unknown transformer: {name}")

        module_name, class_name = transformers[name]
        constructor = _load_constructor(module_name, class_name)
        return constructor(**kwargs)


def _lazy_import_object(path: str) -> object:
    """Import an object by dotted path."""
    raw = (path or "").strip()
    if not raw:
        raise BrainRegistryError("Empty import path")
    if ":" in raw:
        mod_name, attr = raw.split(":", 1)
    elif "." in raw:
        mod_name, attr = raw.rsplit(".", 1)
    else:
        mod_name = raw
        attr = raw
    mod = importlib.import_module(mod_name)
    return _module_symbol(mod.__name__, attr)


class Registry:
    """Minimal registry for dynamic component registration."""

    def __init__(self, *, name: str, builtin_map: dict[str, str] | None = None) -> None:
        """Initialize a new instance of Registry."""
        self._name: str = name
        self._items: dict[str, object] = {}
        self._builtin_map: dict[str, str] = builtin_map or {}

    def has(self, key: str) -> bool:
        """Check if a key exists in the registry."""
        k = key.strip()
        return k in self._items or k in self._builtin_map

    def list_keys(self) -> list[str]:
        """List all available keys in the registry."""
        return sorted(set(self._items.keys()) | set(self._builtin_map.keys()))

    def get(self, key: str) -> object:
        """Retrieve the requested operation."""
        k = key.strip()
        if k not in self._items and k in self._builtin_map:
            self._items[k] = _lazy_import_object(self._builtin_map[k])
        if k not in self._items:
            raise KeyError(f"{self._name}: unknown key '{k}'")
        return self._items[k]

    def register(self, key: str, value: object, *, overwrite: bool = False) -> None:
        """Register the specified operation."""
        k = key.strip()
        if not k:
            raise BrainRegistryError(f"{self._name}: registry key must be non-empty")
        if not overwrite and k in self._items:
            raise KeyError(f"{self._name}: '{k}' already registered")
        self._items[k] = value


# Initialize graph registry after Registry class is defined
graph_registry = Registry(name="graphs", builtin_map={})

# ---- Component Registration (Dynamic Registries) ----

# Dynamic registries for hot-swapping components
_provider_registry: dict[str, ComponentConstructor] = {}
_connector_registry: dict[str, ComponentConstructor] = {}
_transformer_registry: dict[str, ComponentConstructor] = {}


def register_agent(name: str) -> Callable[[C], C]:
    """Register an agent class."""

    def decorator(cls: C) -> C:
        agent_registry.register(name, cls, overwrite=True)
        return cls

    return decorator


def register_connector(name: str) -> Callable[[C], C]:
    """Register a connector class for dynamic selection."""

    def decorator(cls: C) -> C:
        factory = _constructor_for(cls)
        _connector_registry[name] = factory
        ComponentFactory.register_connector_factory(name, factory)
        return cls

    return decorator


def register_provider(name: str) -> Callable[[C], C]:
    """Register a provider class for dynamic selection."""

    def decorator(cls: C) -> C:
        factory = _constructor_for(cls)
        _provider_registry[name] = factory
        ComponentFactory.register_provider_factory(name, factory)
        return cls

    return decorator


def register_transformer(name: str) -> Callable[[C], C]:
    """Register a transformer class for dynamic selection."""

    def decorator(cls: C) -> C:
        factory = _constructor_for(cls)
        _transformer_registry[name] = factory
        ComponentFactory.register_transformer_factory(name, factory)
        return cls

    return decorator


# ---- Dynamic Selection Functions ----


def select_provider(name: str, **kwargs: object) -> object:
    """Dynamically select a provider from registry."""
    if name in _provider_registry:
        return _provider_registry[name](**kwargs)
    return ComponentFactory.create_provider(name, **kwargs)


def select_connector(name: str, **kwargs: object) -> object:
    """Dynamically select a connector from registry."""
    if name in _connector_registry:
        return _connector_registry[name](**kwargs)
    return ComponentFactory.create_connector(name, **kwargs)


def select_transformer(name: str, **kwargs: object) -> object:
    """Dynamically select a transformer from registry."""
    if name in _transformer_registry:
        return _transformer_registry[name](**kwargs)
    return ComponentFactory.create_transformer(name, **kwargs)


# ---- Agent Registry ----
# Essential for cortex agent hot-swapping and dynamic graph assembly

BUILTIN_AGENTS: dict[str, str] = {
    "extract_query": "contextunity.brain.cortex.nodes.rag_retrieval.extract.ExtractQueryAgent",
    "detect_intent": "contextunity.brain.cortex.nodes.rag_retrieval.intent.DetectIntentAgent",
    "retrieve": "contextunity.brain.cortex.nodes.rag_retrieval.retrieve.RetrieveAgent",
    "suggest": "contextunity.brain.cortex.nodes.rag_retrieval.suggest.SuggestAgent",
    "generate": "contextunity.brain.cortex.nodes.rag_retrieval.generate.GenerateAgent",
    "routing": "contextunity.brain.cortex.nodes.rag_retrieval.routing.RoutingAgent",
}

agent_registry: Registry = Registry(name="agents", builtin_map=BUILTIN_AGENTS)

# ---- Plugin scanning -------------------------------------------------------

logger = get_contextunit_logger(__name__)


def scan(plugin_dir: Path) -> None:
    """Scan a directory for Python plugins and import them."""
    if not plugin_dir.exists() or not plugin_dir.is_dir():
        logger.debug("Plugin directory does not exist: %s", plugin_dir)
        return

    plugin_files = list(plugin_dir.glob("*.py"))
    if not plugin_files:
        logger.debug("No Python files found in plugin directory: %s", plugin_dir)
        return

    logger.info("Scanning %d plugin files in %s", len(plugin_files), plugin_dir)

    for plugin_file in plugin_files:
        if plugin_file.name.startswith("_"):
            continue

        try:
            module_name = plugin_file.stem
            spec = importlib.util.spec_from_file_location(module_name, plugin_file)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                logger.info("Loaded plugin: %s from %s", module_name, plugin_file)
            else:
                logger.warning("Could not load plugin: %s", plugin_file)
        except Exception as e:
            logger.error("Failed to load plugin %s: %s", plugin_file, e)


__all__ = [
    "ComponentFactory",
    "agent_registry",
    "graph_registry",
    "register_agent",
    "register_connector",
    "register_graph",
    "register_provider",
    "register_transformer",
    "select_provider",
    "select_connector",
    "scan",
    "select_transformer",
]
