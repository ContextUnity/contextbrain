"""Registry contract tests for dynamic Brain components."""

from __future__ import annotations

from contextunity.brain.core import registry


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
