from __future__ import annotations

import inspect

from contextbrain.core import exceptions as exc


def test_all_contextbrain_errors_have_codes() -> None:
    classes = [
        obj
        for _name, obj in inspect.getmembers(exc, inspect.isclass)
        if issubclass(obj, exc.ContextbrainError)
    ]
    assert classes, "expected at least one ContextbrainError subclass"

    for cls in classes:
        # code must exist and be stable non-empty string
        code = getattr(cls, "code", None)
        assert isinstance(code, str) and code.strip(), f"{cls.__name__}.code must be non-empty str"


def test_error_registry_contains_base_codes() -> None:
    reg = exc.error_registry.all()
    for code in [
        "INTERNAL_ERROR",
        "CONFIGURATION_ERROR",
        "RETRIEVAL_ERROR",
        "INTENT_ERROR",
        "PROVIDER_ERROR",
        "CONNECTOR_ERROR",
        "MODEL_ERROR",
    ]:
        assert code in reg, f"missing {code} in error_registry"
