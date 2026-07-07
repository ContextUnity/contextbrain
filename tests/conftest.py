"""Pytest bootstrap for Brain service tests."""

from __future__ import annotations

import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
tests_dir = str(TESTS_DIR)

if tests_dir not in sys.path:
    sys.path.insert(0, tests_dir)
