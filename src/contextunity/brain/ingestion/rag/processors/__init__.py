"""Processors for ingestion pipeline."""

from __future__ import annotations

from pathlib import Path

from contextunity.brain.core import BrainConfig

from ..core import RawData


def generate_persona_profile(
    all_data: list[RawData],
    output_path: Path,
    *,
    core_cfg: BrainConfig,
    persona_name: str = "Speaker Name",
    bio_text: str | None = None,
    sample_count: int = 50,
    max_chars_per_sample: int = 500,
    max_output_tokens: int = 8192,
) -> None:
    """Lazy import wrapper to avoid circular imports at module import time."""
    from .style import generate_persona_profile as _impl

    return _impl(
        all_data,
        output_path,
        core_cfg=core_cfg,
        persona_name=persona_name,
        bio_text=bio_text,
        sample_count=sample_count,
        max_chars_per_sample=max_chars_per_sample,
        max_output_tokens=max_output_tokens,
    )


__all__ = ["generate_persona_profile"]
