"""Modular configuration system for contextunity.brain."""

from contextunity.core.config import (
    get_bool_env,
    get_env,
)

from .main import (
    BrainConfig,
    get_core_config,
    load_config,
    reset_core_config,
    set_core_config,
)
from .models import (
    ModelsConfig,
)
from .providers import (
    LocalOpenAIConfig,
    OpenAIConfig,
    PostgresConfig,
)

__all__ = [
    # Main classes
    "BrainConfig",
    # Main functions
    "get_core_config",
    "load_config",
    "reset_core_config",
    "set_core_config",
    # Base utilities
    "get_env",
    "get_bool_env",
    # Model configs
    "ModelsConfig",
    # Provider configs
    "OpenAIConfig",
    "LocalOpenAIConfig",
    "PostgresConfig",
]
