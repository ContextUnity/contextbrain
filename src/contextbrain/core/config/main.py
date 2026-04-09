"""Main configuration class that combines all config modules."""

import tomllib
from pathlib import Path

from contextcore import get_context_unit_logger
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from .base import get_bool_env, get_env, set_env_default
from .ingestion import RAGConfig
from .models import LLMConfig, ModelsConfig, RouterConfig
from .paths import ConfigPaths
from .providers import (
    LocalOpenAIConfig,
    OpenAIConfig,
    PostgresConfig,
    VertexConfig,
)
from .security import SecurityConfig


class Config(BaseModel):
    """Main configuration class for ContextBrain.

    This combines all configuration modules into a single, hierarchical structure.
    Configuration is loaded from multiple sources in priority order:
    1. Environment variables
    2. TOML configuration files
    3. Default values
    """

    model_config = ConfigDict(extra="ignore")

    # Core settings
    debug: bool = False
    log_level: str = "INFO"

    # Server / service settings
    port: int = 50051
    instance_name: str = "shared"
    schema_name: str = "brain"
    database_url: str = ""
    tenants: list[str] = Field(default_factory=list)
    embedder_type: str = ""  # "openai", "local", or "" (auto-detect)
    redis_url: str = ""  # Redis URL for embedding cache (fallback: in-memory)
    project_path: str = ""

    # Sub-configurations
    models: ModelsConfig = Field(default_factory=ModelsConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    rag: RAGConfig = Field(default_factory=RAGConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    ingestion: RAGConfig = Field(default_factory=RAGConfig)

    # Provider configurations
    vertex: VertexConfig = Field(default_factory=VertexConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    local: LocalOpenAIConfig = Field(default_factory=LocalOpenAIConfig)

    # Internal state
    paths_cache: ConfigPaths | None = None
    loaded_from: list[Path] = Field(default_factory=list)

    @property
    def paths(self) -> ConfigPaths:
        """Get configuration paths."""
        if self.paths_cache is None:
            # Try to find project root
            from pathlib import Path

            # Look for pyproject.toml or go up from current file
            current = Path(__file__).resolve()
            for parent in [current.parent] + list(current.parents):
                if (parent / "pyproject.toml").exists():
                    self.paths_cache = ConfigPaths.from_root(parent)
                    break
            else:
                # Fallback to current directory
                self.paths_cache = ConfigPaths.from_root(Path.cwd())

        return self.paths_cache

    @classmethod
    def load(cls, config_path: Path | str | None = None) -> "Config":
        """Load configuration from files and environment."""
        # Force Vertex AI mode for langchain-google-genai / google-genai SDK.
        # Without this, the SDK may try API-key auth and fail with:
        # "Could not resolve API token from the environment".
        # Must be set before any ChatGoogleGenerativeAI instance is created.
        set_env_default("GOOGLE_GENAI_USE_VERTEXAI", "true")

        config = cls()
        paths = config.paths

        # Deterministic `.env` loading: only load from the detected project root
        # (e.g. `<repo>/contextbrain/.env`). This avoids accidental cross-repo leakage.
        if paths.env_file.exists():
            load_dotenv(paths.env_file, override=False)

        # Optional explicit override for core config path.
        # This is intentionally separate from ingestion's CONTEXTBRAIN_CONFIG_PATH.
        core_config_path = get_env("CONTEXTBRAIN_CORE_CONFIG_PATH")

        # Load from TOML if available
        toml_path = (
            Path(config_path)
            if config_path
            else (Path(core_config_path).resolve() if core_config_path else paths.toml_config)
        )
        if toml_path.exists():
            try:
                with open(toml_path, "rb") as f:
                    toml_data = tomllib.load(f)

                # Remove read-only properties (like 'paths')
                toml_data.pop("paths", None)

                # Merge TOML data with defaults using Pydantic
                config_dict = config.model_dump()
                config_dict.update(toml_data)
                config = cls.model_validate(config_dict)

                # Restore paths_cache to avoid recomputation
                config.paths_cache = paths
                config.loaded_from.append(toml_path)
            except Exception as e:
                logger = get_context_unit_logger(__name__)
                logger.warning(f"Failed to load TOML config from {toml_path}: {e}")

        # Override with environment variables
        config._apply_env_overrides()

        return config

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to config."""
        # Model configuration
        if llm_val := get_env("CONTEXTBRAIN_DEFAULT_LLM"):
            self.models.default_llm = llm_val
        if intent_val := get_env("CONTEXTBRAIN_INTENT_LLM"):
            self.models.rag.intent.model = intent_val
        if suggestions_val := get_env("CONTEXTBRAIN_SUGGESTIONS_LLM"):
            self.models.rag.suggestions.model = suggestions_val
        if generation_val := get_env("CONTEXTBRAIN_GENERATION_LLM"):
            self.models.rag.generation.model = generation_val
        if no_results_val := get_env("CONTEXTBRAIN_NO_RESULTS_LLM"):
            self.models.rag.no_results.model = no_results_val

        # Vertex configuration
        # Primary (in-repo) env names:
        # - VERTEX_PROJECT_ID
        # - VERTEX_LOCATION
        #
        # Optional host/embedding alias (e.g. when contextbrain is used as a library):
        # - CONTEXTBRAIN_VERTEX_PROJECT_ID
        # - CONTEXTBRAIN_VERTEX_LOCATION
        if project_id := (
            get_env("VERTEX_PROJECT_ID") or get_env("CONTEXTBRAIN_VERTEX_PROJECT_ID")
        ):
            self.vertex.project_id = project_id
        if location := (get_env("VERTEX_LOCATION") or get_env("CONTEXTBRAIN_VERTEX_LOCATION")):
            self.vertex.location = location
        # Vertex AI Search / Discovery Engine location (separate from Vertex LLM region).
        if v := (
            get_env("VERTEX_DISCOVERY_ENGINE_LOCATION")
            or get_env("CONTEXTBRAIN_VERTEX_DISCOVERY_ENGINE_LOCATION")
        ):
            self.vertex.discovery_engine_location = v
        if v := (
            get_env("VERTEX_DATA_STORE_LOCATION")
            or get_env("CONTEXTBRAIN_VERTEX_DATA_STORE_LOCATION")
        ):
            self.vertex.data_store_location = v
        if credentials_path := (
            get_env("VERTEX_CREDENTIALS_PATH") or get_env("CONTEXTBRAIN_VERTEX_CREDENTIALS_PATH")
        ):
            self.vertex.credentials_path = credentials_path

        # Postgres configuration
        if dsn := get_env("POSTGRES_DSN"):
            self.postgres.dsn = dsn
        if v := get_env("POSTGRES_POOL_MIN_SIZE"):
            try:
                self.postgres.pool_min_size = max(1, int(v))
            except ValueError:
                pass
        if v := get_env("POSTGRES_POOL_MAX_SIZE"):
            try:
                self.postgres.pool_max_size = max(1, int(v))
            except ValueError:
                pass
        if v := get_env("POSTGRES_RLS_ENABLED"):
            self.postgres.rls_enabled = v.lower() in {"1", "true", "yes", "on"}
        if v := get_env("PGVECTOR_DIM"):
            try:
                self.postgres.vector_dim = max(1, int(v))
            except ValueError:
                pass

        # OpenAI configuration
        if openai_key := get_env("OPENAI_API_KEY"):
            self.openai.api_key = openai_key
        if openai_org := get_env("OPENAI_ORGANIZATION"):
            self.openai.organization = openai_org
        if openai_embedding_model := get_env("OPENAI_EMBEDDING_MODEL"):
            self.openai.embedding_model = openai_embedding_model

        # Local OpenAI-compatible servers (vLLM/Ollama)
        if v := get_env("LOCAL_OLLAMA_BASE_URL"):
            self.local.ollama_base_url = v
        if v := get_env("LOCAL_VLLM_BASE_URL"):
            self.local.vllm_base_url = v

        # Use shared configuration from contextcore where applicable
        from contextcore.config import get_core_config as get_shared_core_config

        shared_config = get_shared_core_config()

        # Security: private_key_path removed — signing is
        # handled by contextcore.signing backends (auto-detected).

        # Debug/Logging
        if debug_val := get_bool_env("CONTEXTBRAIN_DEBUG"):
            self.debug = debug_val

        # ContextBrain specific log level overrides shared core log level
        if log_level := get_env("CONTEXTBRAIN_LOG_LEVEL"):
            self.log_level = log_level
        else:
            self.log_level = shared_config.log_level

        # Server / service settings
        if v := get_env("BRAIN_PORT"):
            try:
                self.port = int(v)
            except ValueError:
                pass
        if v := get_env("BRAIN_INSTANCE_NAME"):
            self.instance_name = v
        if v := get_env("BRAIN_SCHEMA"):
            self.schema_name = v
        if v := get_env("BRAIN_DATABASE_URL") or get_env("DATABASE_URL"):
            self.database_url = v
        if v := get_env("BRAIN_TENANTS"):
            self.tenants = [t.strip() for t in v.split(",") if t.strip()]
        if v := get_env("EMBEDDER_TYPE"):
            self.embedder_type = v.lower()

        # Redis config (from shared core)
        if shared_config.redis_url:
            self.redis_url = shared_config.redis_url

        if v := get_env("CONTEXTBRAIN_PROJECT_PATH"):
            self.project_path = v


# ---- Global config management ----

_GLOBAL_CONFIG: Config | None = None


def get_core_config() -> Config:
    """Return process-global core config (for framework modules)."""
    global _GLOBAL_CONFIG
    if _GLOBAL_CONFIG is None:
        _GLOBAL_CONFIG = Config.load()
    return _GLOBAL_CONFIG


def set_core_config(config: Config) -> None:
    """Set the global core config."""
    global _GLOBAL_CONFIG
    _GLOBAL_CONFIG = config
