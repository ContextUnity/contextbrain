"""LLM utilities for ingestion (no env access, no side effects).

NOTE: This module requires model registry which is not yet implemented.
Functions will raise NotImplementedError until model registry is available.
"""

from __future__ import annotations

import asyncio
import logging

from contextbrain.core import Config

logger = logging.getLogger(__name__)


def _resolve_json_model(core_cfg: Config, model: str) -> str:
    """Resolve model for JSON-critical ingestion steps via config override."""
    json_model = core_cfg.models.ingestion.json_model.model.strip()
    if json_model:
        return json_model
    return model


def llm_generate(
    *,
    core_cfg: Config,
    prompt: str,
    model: str,
    max_tokens: int = 16384,
    temperature: float = 0.1,
    max_retries: int = 5,
    parse_json: bool = True,
) -> dict[str, object] | list[object] | str:
    """Generate using a chat model (synchronous wrapper)."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            _llm_generate_impl(
                core_cfg=core_cfg,
                prompt=prompt,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                max_retries=max_retries,
                parse_json=parse_json,
            )
        )

    # Running loop exists. This function is intentionally synchronous; do not nest event loops.
    raise RuntimeError(
        "llm_generate() is synchronous and cannot run inside an active asyncio loop. "
        "Call `await llm_generate_async(...)` instead."
    )


async def llm_generate_async(
    *,
    core_cfg: Config,
    prompt: str,
    model: str,
    max_tokens: int = 16384,
    temperature: float = 0.1,
    max_retries: int = 5,
    parse_json: bool = True,
) -> dict[str, object] | list[object] | str:
    """Async version of llm_generate(). Safe to call from within an event loop."""
    return await _llm_generate_impl(
        core_cfg=core_cfg,
        prompt=prompt,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        max_retries=max_retries,
        parse_json=parse_json,
    )


async def _llm_generate_impl(
    *,
    core_cfg: Config,
    prompt: str,
    model: str,
    max_tokens: int,
    temperature: float,
    max_retries: int,
    parse_json: bool,
) -> dict[str, object] | list[object] | str:
    """LLM generation implementation - requires model registry."""
    if parse_json:
        model = _resolve_json_model(core_cfg, model)

    # TODO: Implement model registry or use alternative LLM interface
    raise NotImplementedError(
        "LLM generation requires model registry which is not yet implemented in contextbrain. "
        f"Requested model: {model}. "
        "This functionality will be available once model registry is implemented."
    )


__all__ = ["llm_generate", "llm_generate_async"]
