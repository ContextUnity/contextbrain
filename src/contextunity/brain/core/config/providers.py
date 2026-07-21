"""Strict provider configurations used by Brain-owned embedding generation."""

from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from contextunity.brain.embedding_space import DEFAULT_EMBEDDING_DIMENSION


class PostgresConfig(BaseModel):
    """Configuration settings for PostgresConfig."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 10
    rls_enabled: bool = True
    vector_dim: int = DEFAULT_EMBEDDING_DIMENSION


EmbeddingProviderKind = Literal[
    "onnx",
    "openai",
    "ollama",
    "vllm",
    "sentence_transformers",
    "deterministic",
]

_ONNX_EMBEDDINGGEMMA_MODEL = "onnx-community/embeddinggemma-300m-ONNX"
# Existing durable vectors were written in this space. A role-prefixed model
# upgrade requires a separately owned reindex/storage-isolation migration.
_ONNX_EMBEDDINGGEMMA_SPACE = "embeddinggemma-300m-onnx-768-v1"
_EXTERNAL_PROVIDERS = frozenset({"openai", "ollama", "vllm"})
_LOCAL_PROVIDERS = frozenset({"onnx", "sentence_transformers"})


class EmbeddingProviderConfig(BaseModel):
    """Operator-owned embedding provider profile.

    ``endpoint`` is the complete embeddings endpoint for HTTP providers.  This
    prevents provider selection from guessing URL shapes such as ``/v1`` or
    ``/api/embed``. Credentials are resolved from C0 configuration only and
    never supplied by Router, CLI, or Worker requests.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    provider: EmbeddingProviderKind = "onnx"
    space_id: str = Field(
        default=_ONNX_EMBEDDINGGEMMA_SPACE,
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    model: str = Field(default=_ONNX_EMBEDDINGGEMMA_MODEL, min_length=1, max_length=256)
    dimension: int = Field(default=DEFAULT_EMBEDDING_DIMENSION, ge=1, le=16_384)
    endpoint: str | None = Field(default=None, max_length=2_048)
    api_key: SecretStr | None = None
    device: Literal["auto", "cpu", "cuda", "coreml", "dml"] = "auto"
    model_cache_dir: str | None = Field(default=None, min_length=1, max_length=2_048)
    onnx_intra_op_threads: int = Field(default=2, ge=1, le=64)
    onnx_cpu_mem_arena: bool = False
    onnx_mem_pattern: bool = False

    @model_validator(mode="after")
    def validate_provider_settings(self) -> "EmbeddingProviderConfig":
        """Reject ambiguous or unreachable provider profiles at config load."""
        if self.provider != "onnx" and (
            self.onnx_intra_op_threads != 2
            or self.onnx_cpu_mem_arena is not False
            or self.onnx_mem_pattern is not False
        ):
            raise ValueError(
                f"embeddings.onnx_* settings require provider=onnx; provider={self.provider}"
            )
        if self.provider in _EXTERNAL_PROVIDERS:
            if not self.endpoint:
                raise ValueError(f"embeddings.endpoint is required for provider={self.provider}")
            if self.model == _ONNX_EMBEDDINGGEMMA_MODEL:
                raise ValueError(f"embeddings.model must be explicit for provider={self.provider}")
            if self.space_id == _ONNX_EMBEDDINGGEMMA_SPACE:
                raise ValueError(
                    f"embeddings.space_id must be explicit for provider={self.provider}"
                )
            if self.device != "auto" or self.model_cache_dir is not None:
                raise ValueError(
                    "embeddings.device, embeddings.model_cache_dir, and embeddings.onnx_* "
                    f"settings apply only to local providers; provider={self.provider}"
                )
        elif self.provider in _LOCAL_PROVIDERS:
            if self.endpoint is not None or self.api_key is not None:
                raise ValueError(
                    f"embeddings.endpoint and embeddings.api_key are not valid for provider={self.provider}"
                )
            if (
                self.provider == "sentence_transformers"
                and self.space_id == _ONNX_EMBEDDINGGEMMA_SPACE
            ):
                raise ValueError("embeddings.space_id must be explicit for sentence_transformers")
        elif self.provider == "deterministic":
            if self.endpoint is not None or self.api_key is not None:
                raise ValueError("deterministic embeddings do not accept endpoint or api_key")
            if self.space_id == _ONNX_EMBEDDINGGEMMA_SPACE:
                raise ValueError("embeddings.space_id must be explicit for deterministic")
        return self


__all__ = [
    "EmbeddingProviderConfig",
    "EmbeddingProviderKind",
    "PostgresConfig",
]
