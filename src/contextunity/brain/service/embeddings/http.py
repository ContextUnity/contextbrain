"""Explicit HTTP embedding provider adapters."""

from __future__ import annotations

from contextunity.core.narrowing import as_json_dict
from contextunity.core.parsing import json_loads
from contextunity.core.types import is_object_list

from contextunity.brain.core.config.providers import EmbeddingProviderConfig
from contextunity.brain.core.exceptions import EmbeddingError

from .cache import EmbeddingCache, get_embedding_cache
from .contracts import run_coroutine_sync, validate_embedding_vector


def _number_list(raw: object) -> list[float] | None:
    if not is_object_list(raw):
        return None
    values: list[float] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return None
        values.append(float(item))
    return values


def _parse_http_embedding(payload: object, *, provider: str) -> list[float] | None:
    data = as_json_dict(payload)
    if provider == "ollama":
        single = _number_list(data.get("embedding"))
        if single is not None:
            return single
        rows = data.get("embeddings")
        if not isinstance(rows, list) or not rows:
            return None
        return _number_list(rows[0])
    rows = data.get("data")
    if not isinstance(rows, list) or not rows:
        return None
    return _number_list(as_json_dict(rows[0]).get("embedding"))


class HttpEmbedder:
    """Provider adapter for explicitly configured OpenAI-compatible HTTP APIs."""

    def __init__(
        self, config: EmbeddingProviderConfig, *, cache: EmbeddingCache | None = None
    ) -> None:
        self._config = config
        self._cache = cache or get_embedding_cache()
        self._identity = f"{config.space_id}:{config.provider}:{config.model}:{config.dimension}"

    def embed(self, text: str) -> list[float]:
        """Synchronously generate one vector."""
        return run_coroutine_sync(lambda: self.embed_async(text))

    async def embed_query_async(self, text: str) -> list[float]:
        """Delegate query roles to the explicitly configured remote model."""
        return await self.embed_async(text)

    async def embed_document_async(self, text: str) -> list[float]:
        """Delegate document roles to the explicitly configured remote model."""
        return await self.embed_async(text)

    async def embed_async(self, text: str) -> list[float]:
        """Generate and validate one vector through the configured endpoint."""
        cached = await self._cache.get(self._identity, text)
        if cached is not None:
            return validate_embedding_vector(cached, config=self._config)
        try:
            import httpx

            headers = {"Content-Type": "application/json"}
            if self._config.api_key is not None:
                headers["Authorization"] = f"Bearer {self._config.api_key.get_secret_value()}"
            request_payload: dict[str, str] = {"model": self._config.model, "input": text}
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self._config.endpoint or "",
                    headers=headers,
                    json=request_payload,
                    timeout=30.0,
                )
                _ = response.raise_for_status()
            vector = _parse_http_embedding(
                json_loads(response.text), provider=self._config.provider
            )
            if vector is None:
                raise EmbeddingError("HTTP embedding response does not contain one numeric vector")
            vector = validate_embedding_vector(vector, config=self._config)
            await self._cache.put(self._identity, text, vector)
            return vector
        except EmbeddingError:
            raise
        except Exception as exc:
            raise EmbeddingError(f"HTTP embedding request failed: {type(exc).__name__}") from exc


__all__ = ["HttpEmbedder"]
