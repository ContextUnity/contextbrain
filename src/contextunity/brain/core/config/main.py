"""Main configuration class that combines all config modules."""

from typing import ClassVar

from contextunity.core.config import ServiceConfig
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import ModelsConfig
from .providers import (
    EmbeddingProviderConfig,
    PostgresConfig,
)


class SynapsesConfig(BaseModel):
    """BrainSynapse rollout flags.

    Both default ``False`` until their respective test chains pass; rollback
    is flag-off — existing ``synapses`` rows stay readable either way.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    enabled: bool = False
    decay_enabled: bool = False


class EmbeddingEnrichmentConfig(BaseModel):
    """Bounded, operator-controlled asynchronous embedding settings."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    enabled: bool = False
    max_input_chars: int = Field(default=12000, ge=1, le=1_000_000)
    max_pending_per_tenant: int = Field(default=10_000, ge=1, le=1_000_000)
    lease_seconds: int = Field(default=300, ge=5, le=86_400)


class BrainConfig(ServiceConfig):
    """Main configuration class for contextunity.brain."""

    model_config: ClassVar[ConfigDict] = ConfigDict(use_enum_values=True, extra="forbid")

    # Core settings
    debug: bool = False

    # Server / service settings
    port: int = 50051
    instance_name: str = "shared"
    schema_name: str = "brain"
    tenants: list[str] = Field(default_factory=list)
    project_path: str = ""

    # Sub-configurations
    models: ModelsConfig = Field(default_factory=ModelsConfig)

    # Provider configurations
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    embeddings: EmbeddingProviderConfig = Field(default_factory=EmbeddingProviderConfig)

    # Phase 2 BrainSynapse rollout flags
    synapses: SynapsesConfig = Field(default_factory=SynapsesConfig)
    embedding_enrichment: EmbeddingEnrichmentConfig = Field(
        default_factory=EmbeddingEnrichmentConfig
    )

    @model_validator(mode="after")
    def validate_embedding_dimension(self) -> "BrainConfig":
        """Keep the configured provider vector space compatible with storage."""
        if self.postgres.vector_dim != self.embeddings.dimension:
            raise ValueError(
                "postgres.vector_dim must equal embeddings.dimension "
                f"({self.postgres.vector_dim} != {self.embeddings.dimension})"
            )
        if self.embeddings.provider == "deterministic" and not self.debug:
            raise ValueError("deterministic embeddings are permitted only when brain.debug=true")
        return self


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
        # Brain-owned embedding provider. HTTP endpoints are complete
        # embeddings endpoints; provider-specific URL guessing is forbidden.
        "CU_BRAIN_EMBEDDING_PROVIDER": "embeddings.provider",
        "CU_BRAIN_EMBEDDING_PROFILE": "embeddings.space_id",
        "CU_BRAIN_EMBEDDING_MODEL": "embeddings.model",
        "CU_BRAIN_EMBEDDING_DIMENSION": "embeddings.dimension",
        "CU_BRAIN_EMBEDDING_ENDPOINT": "embeddings.endpoint",
        "CU_BRAIN_EMBEDDING_API_KEY": "embeddings.api_key",
        "CU_BRAIN_EMBEDDING_DEVICE": "embeddings.device",
        "CU_BRAIN_EMBEDDING_MODEL_CACHE_DIR": "embeddings.model_cache_dir",
        # Server settings
        "CU_BRAIN_DEBUG": "debug",
        "BRAIN_INSTANCE_NAME": "instance_name",
        "BRAIN_SCHEMA": "schema_name",
        "CU_BRAIN_PROJECT_PATH": "project_path",
        # Phase 2 BrainSynapse rollout flags
        "CU_BRAIN_SYNAPSES_ENABLED": "synapses.enabled",
        "CU_BRAIN_SYNAPSES_DECAY_ENABLED": "synapses.decay_enabled",
        "CU_BRAIN_EMBEDDING_ENRICHMENT_ENABLED": "embedding_enrichment.enabled",
        "CU_BRAIN_EMBEDDING_MAX_INPUT_CHARS": "embedding_enrichment.max_input_chars",
        "CU_BRAIN_EMBEDDING_MAX_PENDING": "embedding_enrichment.max_pending_per_tenant",
        "CU_BRAIN_EMBEDDING_LEASE_SECONDS": "embedding_enrichment.lease_seconds",
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
