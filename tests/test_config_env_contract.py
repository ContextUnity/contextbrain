"""Brain config environment contract regressions."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from contextunity.brain.core.config import get_core_config, reset_core_config
from contextunity.brain.core.config.main import BrainConfig


def test_brain_postgres_dsn_is_canonical(monkeypatch):
    """POSTGRES_DSN is canonical for brain database connection."""
    reset_core_config()
    monkeypatch.setenv("POSTGRES_DSN", "postgres-specific")

    cfg = get_core_config()

    assert cfg.postgres.dsn == "postgres-specific"
    reset_core_config()


def test_brain_synapses_rollout_flag_defaults_off_and_env_enables(monkeypatch):
    reset_core_config()
    assert get_core_config().synapses.enabled is False

    reset_core_config()
    monkeypatch.setenv("CU_BRAIN_SYNAPSES_ENABLED", "true")

    assert get_core_config().synapses.enabled is True
    reset_core_config()


def test_brain_onnx_memory_settings_resolve_from_env(monkeypatch) -> None:
    reset_core_config()
    monkeypatch.setenv("CU_BRAIN_ONNX_INTRA_OP_THREADS", "4")
    monkeypatch.setenv("CU_BRAIN_ONNX_CPU_MEM_ARENA", "true")
    monkeypatch.setenv("CU_BRAIN_ONNX_MEM_PATTERN", "true")

    cfg = get_core_config()

    assert cfg.embeddings.onnx_intra_op_threads == 4
    assert cfg.embeddings.onnx_cpu_mem_arena is True
    assert cfg.embeddings.onnx_mem_pattern is True
    reset_core_config()


def test_brain_config_rejects_unknown_security_field() -> None:
    with pytest.raises(ValidationError, match="tls_require_client_authx"):
        BrainConfig.model_validate({"tls_require_client_authx": False})
