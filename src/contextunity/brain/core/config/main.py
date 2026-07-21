"""Main configuration class that combines all config modules."""

from typing import ClassVar

from contextunity.core.config import ServiceConfig, ServiceConfigRegistry
from contextunity.core.sdk.execution_trace_artifacts import ProtectedModelIOSettings
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

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    enabled: bool = False
    decay_enabled: bool = False
    outcome_resolver_enabled: bool = False
    outcome_policy_version: str = Field(
        default="contextunity.outcome-resolution/v1",
        pattern=r"contextunity\.outcome-resolution/v[1-9][0-9]*",
    )


class UdbConfig(BaseModel):
    """Operator-owned UniversalDebugBus rollout and bounded query settings."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    enabled: bool = False
    max_query_limit: int = Field(default=100, ge=1, le=100)


class EmbeddingEnrichmentConfig(BaseModel):
    """Bounded, operator-controlled asynchronous embedding settings."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    enabled: bool = False
    max_input_chars: int = Field(default=12000, ge=1, le=1_000_000)
    max_pending_per_tenant: int = Field(default=10_000, ge=1, le=1_000_000)
    lease_seconds: int = Field(default=300, ge=5, le=86_400)


class BrainReadBulkheadConfig(BaseModel):
    """Independent service-side bounds for canonical memory retrieval RPCs."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    global_limit: int = Field(default=64, ge=1, le=10_000)
    per_tenant_limit: int = Field(default=16, ge=1, le=4096)
    deadline_ms: int = Field(default=5_000, ge=1, le=120_000)
    max_queue: int = Field(default=128, ge=0, le=20_000)
    per_tenant_queue_limit: int = Field(default=32, ge=0, le=2_000)

    @model_validator(mode="after")
    def _queue_bounds(self) -> "BrainReadBulkheadConfig":
        if self.per_tenant_queue_limit > self.max_queue:
            raise ValueError("Brain read per-tenant queue cannot exceed global queue")
        return self


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
    sqlite_path: str = "~/.contextunity/brain_local.sqlite3"

    # Sub-configurations
    models: ModelsConfig = Field(default_factory=ModelsConfig)

    # Provider configurations
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    embeddings: EmbeddingProviderConfig = Field(default_factory=EmbeddingProviderConfig)

    # Phase 2 BrainSynapse rollout flags
    synapses: SynapsesConfig = Field(default_factory=SynapsesConfig)
    udb: UdbConfig = Field(default_factory=UdbConfig)
    embedding_enrichment: EmbeddingEnrichmentConfig = Field(
        default_factory=EmbeddingEnrichmentConfig
    )
    trace_artifacts: ProtectedModelIOSettings = Field(default_factory=ProtectedModelIOSettings)
    read_bulkhead: BrainReadBulkheadConfig = Field(default_factory=BrainReadBulkheadConfig)

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
        "CU_BRAIN_ONNX_INTRA_OP_THREADS": "embeddings.onnx_intra_op_threads",
        "CU_BRAIN_ONNX_CPU_MEM_ARENA": "embeddings.onnx_cpu_mem_arena",
        "CU_BRAIN_ONNX_MEM_PATTERN": "embeddings.onnx_mem_pattern",
        # Server settings
        "CU_BRAIN_DEBUG": "debug",
        "BRAIN_INSTANCE_NAME": "instance_name",
        "BRAIN_SCHEMA": "schema_name",
        "CU_BRAIN_PROJECT_PATH": "project_path",
        "CU_BRAIN_SQLITE_PATH": "sqlite_path",
        # Phase 2 BrainSynapse rollout flags
        "CU_BRAIN_SYNAPSES_ENABLED": "synapses.enabled",
        "CU_BRAIN_SYNAPSES_DECAY_ENABLED": "synapses.decay_enabled",
        "CU_BRAIN_OUTCOME_RESOLVER_ENABLED": "synapses.outcome_resolver_enabled",
        "CU_BRAIN_OUTCOME_POLICY_VERSION": "synapses.outcome_policy_version",
        "CU_BRAIN_UDB_ENABLED": "udb.enabled",
        "CU_BRAIN_UDB_MAX_QUERY_LIMIT": "udb.max_query_limit",
        "CU_BRAIN_EMBEDDING_ENRICHMENT_ENABLED": "embedding_enrichment.enabled",
        "CU_BRAIN_EMBEDDING_MAX_INPUT_CHARS": "embedding_enrichment.max_input_chars",
        "CU_BRAIN_EMBEDDING_MAX_PENDING": "embedding_enrichment.max_pending_per_tenant",
        "CU_BRAIN_EMBEDDING_LEASE_SECONDS": "embedding_enrichment.lease_seconds",
        "CU_BRAIN_READ_BULKHEAD_ENABLED": "read_bulkhead.enabled",
        "CU_BRAIN_READ_BULKHEAD_GLOBAL_LIMIT": "read_bulkhead.global_limit",
        "CU_BRAIN_READ_BULKHEAD_PER_TENANT_LIMIT": "read_bulkhead.per_tenant_limit",
        "CU_BRAIN_READ_BULKHEAD_DEADLINE_MS": "read_bulkhead.deadline_ms",
        "CU_BRAIN_READ_BULKHEAD_MAX_QUEUE": "read_bulkhead.max_queue",
        "CU_BRAIN_READ_BULKHEAD_PER_TENANT_QUEUE_LIMIT": ("read_bulkhead.per_tenant_queue_limit"),
    }

    return load_service_config(
        BrainConfig,
        "brain",
        env_mappings=env_mappings,
        config_path=config_path,
    )


_registry = ServiceConfigRegistry(load_config)

get_core_config = _registry.get


def _reset_config_runtime() -> None:
    from contextunity.brain.service.read_bulkhead import reset_brain_read_bulkhead

    reset_brain_read_bulkhead()


def set_core_config(config: BrainConfig) -> None:
    _registry.set(config)
    _reset_config_runtime()


def reset_core_config() -> None:
    _registry.reset()
    _reset_config_runtime()
