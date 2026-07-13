"""Provider selection for Brain-owned embedding generation."""

from __future__ import annotations

from contextunity.brain.core.config.main import BrainConfig

from .contracts import Embedder
from .deterministic import DeterministicEmbedder
from .http import HttpEmbedder
from .onnx import OnnxEmbedder
from .sentence_transformers import SentenceTransformersEmbedder


def get_embedder(config: BrainConfig | None = None) -> Embedder:
    """Resolve exactly one explicitly configured embedding provider."""
    if config is None:
        from contextunity.brain.core.config import get_core_config

        config = get_core_config()
    provider = config.embeddings.provider
    if provider == "onnx":
        return OnnxEmbedder(config.embeddings)
    if provider == "sentence_transformers":
        return SentenceTransformersEmbedder(config.embeddings)
    if provider == "deterministic":
        return DeterministicEmbedder(config.embeddings)
    return HttpEmbedder(config.embeddings)


__all__ = ["get_embedder"]
