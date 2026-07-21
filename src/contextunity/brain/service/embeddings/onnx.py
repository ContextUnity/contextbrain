"""In-process ONNX Runtime adapter for the default local embedding profile."""

from __future__ import annotations

import asyncio
import importlib
import math
from pathlib import Path

from contextunity.core.config import get_env
from contextunity.core.narrowing import object_attr
from contextunity.core.types import is_object_iterable, is_object_list

from contextunity.brain.core.config.providers import EmbeddingProviderConfig
from contextunity.brain.core.exceptions import EmbeddingError

from .cache import EmbeddingCache, get_embedding_cache
from .contracts import run_coroutine_sync, validate_embedding_vector

_MODEL = "onnx-community/embeddinggemma-300m-ONNX"
_MODEL_REVISION = "5090578d9565bb06545b4552f76e6bc2c93e4a66"
_MODEL_FILE = "model_quantized.onnx"
_MODEL_DATA_FILE = "model_quantized.onnx_data"
_TOKENIZER_FILE = "tokenizer.json"
# This is the durable v1 vector-space contract. Do not change it in place:
# prefix changes require storage isolation and a complete reindex first.
_TASK_PREFIX = "task: sentence similarity | query: "
_MAX_TOKENS = 2048


def _call_attribute(target: object, name: str, *args: object, **kwargs: object) -> object:
    """Call a third-party attribute through one typed optional-SDK boundary."""
    function = object_attr(target, name)
    if not callable(function):
        raise EmbeddingError(f"ONNX dependency does not provide {name}()")
    return function(*args, **kwargs)


def _object_items(value: object, *, error: str) -> list[object]:
    if not is_object_iterable(value):
        raise EmbeddingError(error)
    return list(value)


def _integer_items(value: object, *, error: str) -> list[int]:
    values = _object_items(value, error=error)
    integers: list[int] = []
    for item in values:
        if not isinstance(item, int) or isinstance(item, bool):
            raise EmbeddingError(error)
        integers.append(item)
    return integers


def _vector_from_output(value: object) -> list[float]:
    matrix = _call_attribute(value, "tolist")
    if not is_object_list(matrix) or not matrix or not is_object_list(matrix[0]):
        raise EmbeddingError("ONNX embedding model returned a malformed vector")
    row = matrix[0]
    vector: list[float] = []
    for item in row:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise EmbeddingError("ONNX embedding model returned a malformed vector")
        vector.append(float(item))
    return vector


def _execution_providers(device: str) -> list[str]:
    """Resolve an ONNX provider list without silently claiming acceleration."""
    try:
        runtime_module = importlib.import_module("onnxruntime")
    except ImportError as exc:
        raise EmbeddingError(
            "ONNX embeddings require the contextunity-brain ONNX dependencies"
        ) from exc
    available_values = _object_items(
        _call_attribute(runtime_module, "get_available_providers"),
        error="onnxruntime returned malformed execution providers",
    )
    available = {value for value in available_values if isinstance(value, str)}
    candidates = {
        "cuda": "CUDAExecutionProvider",
        "coreml": "CoreMLExecutionProvider",
        "dml": "DmlExecutionProvider",
    }
    if device == "cpu":
        return ["CPUExecutionProvider"]
    if device == "auto":
        for preferred in (
            "CUDAExecutionProvider",
            "CoreMLExecutionProvider",
            "DmlExecutionProvider",
        ):
            if preferred in available:
                return [preferred, "CPUExecutionProvider"]
        return ["CPUExecutionProvider"]
    preferred = candidates[device]
    if preferred not in available:
        raise EmbeddingError(
            f"Configured ONNX device={device} is unavailable; install its runtime or use device=auto"
        )
    return [preferred, "CPUExecutionProvider"]


def _session_options(runtime_module: object, config: EmbeddingProviderConfig) -> object:
    """Build the memory-bounded CPU session used by local Brain embeddings."""
    options = _call_attribute(runtime_module, "SessionOptions")
    setattr(options, "intra_op_num_threads", config.onnx_intra_op_threads)
    setattr(options, "inter_op_num_threads", 1)
    setattr(options, "enable_cpu_mem_arena", config.onnx_cpu_mem_arena)
    setattr(options, "enable_mem_pattern", config.onnx_mem_pattern)
    setattr(
        options,
        "execution_mode",
        object_attr(object_attr(runtime_module, "ExecutionMode"), "ORT_SEQUENTIAL"),
    )
    setattr(
        options,
        "graph_optimization_level",
        object_attr(
            object_attr(runtime_module, "GraphOptimizationLevel"),
            "ORT_ENABLE_ALL",
        ),
    )
    _ = _call_attribute(options, "add_session_config_entry", "session.intra_op.allow_spinning", "0")
    _ = _call_attribute(options, "add_session_config_entry", "session.inter_op.allow_spinning", "0")
    return options


def prefetch_onnx_assets(config: EmbeddingProviderConfig) -> tuple[str, str]:
    """Resolve the pinned graph, external weights, and tokenizer into one cache."""
    if config.model != _MODEL:
        raise EmbeddingError(
            f"Unsupported ONNX embedding model: {config.model}; supported={_MODEL}"
        )
    try:
        hub_module = importlib.import_module("huggingface_hub")
    except ImportError as exc:
        raise EmbeddingError(
            "ONNX embeddings require the contextunity-brain ONNX dependencies"
        ) from exc
    cache_dir = Path(config.model_cache_dir or "~/.cache/contextunity/huggingface").expanduser()
    local_files_only = (get_env("HF_HUB_OFFLINE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    model_path = _call_attribute(
        hub_module,
        "hf_hub_download",
        repo_id=_MODEL,
        revision=_MODEL_REVISION,
        local_files_only=local_files_only,
        subfolder="onnx",
        filename=_MODEL_FILE,
        cache_dir=cache_dir,
    )
    model_data_path = _call_attribute(
        hub_module,
        "hf_hub_download",
        repo_id=_MODEL,
        revision=_MODEL_REVISION,
        local_files_only=local_files_only,
        subfolder="onnx",
        filename=_MODEL_DATA_FILE,
        cache_dir=cache_dir,
    )
    tokenizer_path = _call_attribute(
        hub_module,
        "hf_hub_download",
        repo_id=_MODEL,
        revision=_MODEL_REVISION,
        local_files_only=local_files_only,
        filename=_TOKENIZER_FILE,
        cache_dir=cache_dir,
    )
    if (
        not isinstance(model_path, str)
        or not isinstance(model_data_path, str)
        or not isinstance(tokenizer_path, str)
    ):
        raise EmbeddingError("ONNX model download returned an invalid asset path")
    return model_path, tokenizer_path


class OnnxEmbedder:
    """Lazy EmbeddingGemma ONNX provider with a fixed native 768-dimensional space."""

    def __init__(
        self, config: EmbeddingProviderConfig, *, cache: EmbeddingCache | None = None
    ) -> None:
        self._config = config
        self._cache = cache or get_embedding_cache()
        self._session: object | None = None
        self._tokenizer: object | None = None
        self._output_index: int | None = None
        self._load_lock = asyncio.Lock()
        self._identity = f"{config.space_id}:{config.provider}:{config.model}:{config.dimension}"

    def embed(self, text: str) -> list[float]:
        """Synchronously generate one vector in the durable v1 space."""
        return run_coroutine_sync(lambda: self.embed_async(text))

    async def embed_async(self, text: str) -> list[float]:
        """Generate one normalized embedding without changing the durable space."""
        cached = await self._cache.get(self._identity, text)
        if cached is not None:
            return validate_embedding_vector(cached, config=self._config)
        await self._ensure_loaded()
        vector = await asyncio.to_thread(self._embed_blocking, _TASK_PREFIX + text)
        vector = validate_embedding_vector(vector, config=self._config)
        await self._cache.put(self._identity, text, vector)
        return vector

    async def embed_query_async(self, text: str) -> list[float]:
        """Keep query vectors compatible with existing durable v1 vectors."""
        return await self.embed_async(text)

    async def embed_document_async(self, text: str) -> list[float]:
        """Keep document vectors compatible with existing durable v1 vectors."""
        return await self.embed_async(text)

    async def _ensure_loaded(self) -> None:
        if self._session is not None:
            return
        async with self._load_lock:
            if self._session is None:
                await asyncio.to_thread(self._load_blocking)

    def _load_blocking(self) -> None:
        model_path, tokenizer_path = prefetch_onnx_assets(self._config)
        try:
            runtime_module = importlib.import_module("onnxruntime")
            tokenizer_module = importlib.import_module("tokenizers")
        except ImportError as exc:
            raise EmbeddingError(
                "ONNX embeddings require the contextunity-brain ONNX dependencies"
            ) from exc
        session = _call_attribute(
            runtime_module,
            "InferenceSession",
            model_path,
            sess_options=_session_options(runtime_module, self._config),
            providers=_execution_providers(self._config.device),
        )
        outputs = _object_items(
            _call_attribute(session, "get_outputs"),
            error="ONNX embedding model returned malformed output metadata",
        )
        output_names = [
            name for output in outputs if isinstance(name := object_attr(output, "name"), str)
        ]
        if "sentence_embedding" not in output_names:
            raise EmbeddingError("ONNX embedding model has no sentence_embedding output")
        tokenizer_type = object_attr(tokenizer_module, "Tokenizer")
        tokenizer = _call_attribute(tokenizer_type, "from_file", tokenizer_path)
        _ = _call_attribute(tokenizer, "enable_padding")
        _ = _call_attribute(tokenizer, "enable_truncation", max_length=_MAX_TOKENS)
        self._session = session
        self._tokenizer = tokenizer
        self._output_index = output_names.index("sentence_embedding")

    def _embed_blocking(self, text: str) -> list[float]:
        session = self._session
        tokenizer = self._tokenizer
        output_index = self._output_index
        if session is None or tokenizer is None or output_index is None:
            raise EmbeddingError("ONNX embedding model is not loaded")
        try:
            numpy_module = importlib.import_module("numpy")
        except ImportError as exc:
            raise EmbeddingError("ONNX embeddings require numpy") from exc
        encoding = _call_attribute(tokenizer, "encode", text)
        input_ids = _integer_items(
            object_attr(encoding, "ids"), error="ONNX tokenizer returned invalid input ids"
        )
        attention_mask = _integer_items(
            object_attr(encoding, "attention_mask"),
            error="ONNX tokenizer returned invalid attention mask",
        )
        int64 = object_attr(numpy_module, "int64")
        model_inputs: dict[str, object] = {
            "input_ids": _call_attribute(numpy_module, "asarray", [input_ids], dtype=int64),
            "attention_mask": _call_attribute(
                numpy_module, "asarray", [attention_mask], dtype=int64
            ),
        }
        outputs = _object_items(
            _call_attribute(session, "run", None, model_inputs),
            error="ONNX embedding model returned malformed outputs",
        )
        if output_index >= len(outputs):
            raise EmbeddingError("ONNX embedding model omitted the sentence_embedding output")
        vector = _vector_from_output(outputs[output_index])
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            raise EmbeddingError("ONNX embedding model returned a zero vector")
        return [value / norm for value in vector]


__all__ = ["OnnxEmbedder", "prefetch_onnx_assets"]
