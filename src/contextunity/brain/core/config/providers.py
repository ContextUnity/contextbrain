"""Provider configurations for external services."""

from pydantic import BaseModel, ConfigDict


class VertexConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str = ""
    location: str = "us-central1"
    # Discovery Engine / Vertex AI Search is typically "global" even when Vertex AI LLM is regional.
    # Keep this separate to avoid accidental 0-result queries due to wrong location.
    #
    # Preferred names:
    # - discovery_engine_location: location for Discovery Engine (Vertex AI Search)
    # - data_store_location: alias for orgs that think in datastore terms
    discovery_engine_location: str = "global"  # Default, can be overridden by env
    data_store_location: str = ""  # optional override
    # Credentials can be loaded from:
    # 1. GOOGLE_APPLICATION_CREDENTIALS env var (ADC)
    # 2. `credentials_path` (explicit service account JSON)
    credentials_path: str = ""


class PostgresConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 10
    rls_enabled: bool = True
    vector_dim: int = 768


class OpenAIConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    api_key: str = ""
    organization: str | None = None
    embedding_model: str | None = None


class LocalOpenAIConfig(BaseModel):
    """Base URLs for local OpenAI-compatible servers."""

    model_config = ConfigDict(extra="ignore")

    ollama_base_url: str = "http://localhost:11434/v1"
    vllm_base_url: str = "http://localhost:8000/v1"
