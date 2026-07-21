"""Pytest bootstrap for Brain service tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
tests_dir = str(TESTS_DIR)

if tests_dir not in sys.path:
    sys.path.insert(0, tests_dir)


@pytest.fixture(scope="session")
def brain_test_dsn(pytestconfig: pytest.Config) -> str:
    """Resolve the explicit Brain test endpoint for service-root pytest runs.

    A monorepo-root run may provide ``--brain-test-dsn``; direct Brain service
    runs rely only on the dedicated ``BRAIN_TEST_DSN`` environment contract.
    ``POSTGRES_DSN`` remains production runtime configuration and is never a
    live-test fallback.
    """
    try:
        configured = pytestconfig.getoption("brain_test_dsn")
    except ValueError:
        configured = None
    value = (configured or os.environ.get("BRAIN_TEST_DSN") or "").strip()
    if not value:
        pytest.skip("BRAIN_TEST_DSN not set — skipping live-Postgres Brain test")
    return value
