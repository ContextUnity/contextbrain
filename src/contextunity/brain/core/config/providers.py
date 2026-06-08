"""Provider configurations for external services."""

from typing import ClassVar

from pydantic import BaseModel, ConfigDict


class PostgresConfig(BaseModel):
    """Configuration settings for PostgresConfig."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 10
    rls_enabled: bool = True
    vector_dim: int = 768


class OpenAIConfig(BaseModel):
    """Configuration settings for OpenAIConfig."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    api_key: str = ""
    organization: str | None = None
    embedding_model: str | None = None


class LocalOpenAIConfig(BaseModel):
    """Base URLs for local OpenAI-compatible servers."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    ollama_base_url: str = "http://localhost:11434/v1"
    vllm_base_url: str = "http://localhost:8000/v1"
