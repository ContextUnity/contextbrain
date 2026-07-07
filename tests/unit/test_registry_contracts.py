"""Registry contract tests for dynamic Brain components."""

from __future__ import annotations

import pytest

from contextunity.brain.core import registry
from contextunity.brain.core.exceptions import BrainRegistryError


def test_register_connector_decorator_uses_class_under_definition() -> None:
    """Class decorators must work before the class name is bound in module globals."""
    name = "unit_test_connector_contract"

    try:

        @registry.register_connector(name)
        class ProbeConnector:
            def __init__(self, *, value: str = "") -> None:
                self.value = value

        instance = registry.select_connector(name, value="ok")

        assert isinstance(instance, ProbeConnector)
        assert instance.value == "ok"
    finally:
        registry._connector_registry.pop(name, None)
        registry.ComponentFactory._connector_factories.pop(name, None)


def test_register_provider_decorator_uses_class_under_definition() -> None:
    name = "unit_test_provider_contract"

    try:

        @registry.register_provider(name)
        class ProbeProvider:
            def __init__(self, *, value: str = "") -> None:
                self.value = value

        instance = registry.select_provider(name, value="ok")

        assert isinstance(instance, ProbeProvider)
        assert instance.value == "ok"
    finally:
        registry._provider_registry.pop(name, None)
        registry.ComponentFactory._provider_factories.pop(name, None)


class TestBuiltinProviderFallbacks:
    """`ComponentFactory.create_provider`'s built-in fallback dict previously
    pointed at ``contextunity.brain.modules.providers...`` — a package that
    does not exist in this codebase (the real classes live under
    ``contextunity.brain.storage``). Guards against that path rotting again
    silently."""

    def test_postgres_provider_resolves_to_real_module(self) -> None:
        from contextunity.brain.storage.postgres.provider import PostgresProvider

        instance = registry.ComponentFactory.create_provider("postgres", store=object())
        assert isinstance(instance, PostgresProvider)

    def test_gcs_provider_resolves_to_real_module(self) -> None:
        from contextunity.brain.storage.gcs import GCSProvider

        instance = registry.ComponentFactory.create_provider("gcs")
        assert isinstance(instance, GCSProvider)

    def test_unknown_provider_raises_registry_error_not_import_error(self) -> None:
        with pytest.raises(BrainRegistryError, match="Unknown provider"):
            registry.ComponentFactory.create_provider("nonexistent")

    def test_unknown_connector_raises_registry_error(self) -> None:
        """No built-in connectors exist — the active ingestion architecture
        is ``ingestion/rag/core/registry.py``'s plugin system instead."""
        with pytest.raises(BrainRegistryError, match="Unknown connector"):
            registry.ComponentFactory.create_connector("web")

    def test_unknown_transformer_raises_registry_error(self) -> None:
        with pytest.raises(BrainRegistryError, match="Unknown transformer"):
            registry.ComponentFactory.create_transformer("summarizer")
