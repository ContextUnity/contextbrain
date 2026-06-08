"""Brain config environment contract regressions."""

from __future__ import annotations

from contextunity.brain.core.config import get_core_config, reset_core_config


def test_brain_postgres_dsn_is_canonical(monkeypatch):
    """POSTGRES_DSN is canonical for brain database connection."""
    reset_core_config()
    monkeypatch.setenv("POSTGRES_DSN", "postgres-specific")

    cfg = get_core_config()

    assert cfg.postgres.dsn == "postgres-specific"
    reset_core_config()
