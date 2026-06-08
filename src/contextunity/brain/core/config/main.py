"""Main configuration class that combines all config modules."""

from contextunity.core.config import ServiceConfig
from pydantic import Field

from .models import ModelsConfig
from .providers import (
    LocalOpenAIConfig,
    OpenAIConfig,
    PostgresConfig,
)


class BrainConfig(ServiceConfig):
    """Main configuration class for contextunity.brain."""

    # Core settings
    debug: bool = False

    # Server / service settings
    port: int = 50051
    instance_name: str = "shared"
    schema_name: str = "brain"
    tenants: list[str] = Field(default_factory=list)
    embedder_type: str = ""  # "openai", "local", or "" (auto-detect)
    project_path: str = ""

    # Sub-configurations
    models: ModelsConfig = Field(default_factory=ModelsConfig)

    # Provider configurations
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    local: LocalOpenAIConfig = Field(default_factory=LocalOpenAIConfig)


# ---- Global config management ----


def load_config(config_path: str | None = None) -> BrainConfig:
    """Load brain config through the unified config loader."""
    from contextunity.core.config import load_service_config

    env_mappings = {
        # Postgres configuration
        "POSTGRES_DSN": "postgres.dsn",
        "POSTGRES_POOL_MIN_SIZE": "postgres.pool_min_size",
        "POSTGRES_POOL_MAX_SIZE": "postgres.pool_max_size",
        "POSTGRES_RLS_ENABLED": "postgres.rls_enabled",
        "PGVECTOR_DIM": "postgres.vector_dim",
        # OpenAI configuration
        "OPENAI_API_KEY": "openai.api_key",
        "OPENAI_ORGANIZATION": "openai.organization",
        "OPENAI_EMBEDDING_MODEL": "openai.embedding_model",
        # Local OpenAI-compatible servers
        "LOCAL_OLLAMA_BASE_URL": "local.ollama_base_url",
        "LOCAL_VLLM_BASE_URL": "local.vllm_base_url",
        # Server settings
        "CU_BRAIN_DEBUG": "debug",
        "BRAIN_INSTANCE_NAME": "instance_name",
        "BRAIN_SCHEMA": "schema_name",
        "EMBEDDER_TYPE": "embedder_type",
        "CU_BRAIN_PROJECT_PATH": "project_path",
    }

    return load_service_config(
        BrainConfig,
        "brain",
        env_mappings=env_mappings,
        config_path=config_path,
    )


from contextunity.core.config import ServiceConfigRegistry  # noqa: E402

_registry = ServiceConfigRegistry(load_config)

get_core_config = _registry.get
set_core_config = _registry.set
reset_core_config = _registry.reset
