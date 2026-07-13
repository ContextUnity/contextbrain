"""Brain-owned embedding provider adapters and their strict factory."""

from .cache import EmbeddingCache, get_embedding_cache
from .contracts import Embedder, validate_embedding_vector
from .deterministic import DeterministicEmbedder
from .factory import get_embedder
from .http import HttpEmbedder
from .onnx import OnnxEmbedder
from .sentence_transformers import SentenceTransformersEmbedder

__all__ = [
    "DeterministicEmbedder",
    "Embedder",
    "EmbeddingCache",
    "HttpEmbedder",
    "OnnxEmbedder",
    "SentenceTransformersEmbedder",
    "get_embedder",
    "get_embedding_cache",
    "validate_embedding_vector",
]
