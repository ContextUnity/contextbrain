"""Stage 1b: CleanText -> persona.txt (optional)."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str
from contextunity.core.types import is_object_list

from contextunity.brain.core import BrainConfig

from ..config import get_assets_paths
from ..core.types import RawData
from ..processors.style import (
    generate_persona_profile,
)
from ..settings import RagIngestionConfig
from .store import read_raw_data_jsonl

logger = get_contextunit_logger(__name__)


def build_persona(*, config: RagIngestionConfig, core_cfg: BrainConfig) -> Path | None:
    """Build persona."""
    if not config.persona.enabled:
        logger.info("persona: disabled")
        return None

    paths = get_assets_paths(config)
    assets_folder = paths["assets_folder"]

    output_path = assets_folder / config.persona.output_path
    persona_name = config.persona.persona_name
    tone_globs = config.persona.tone_sample_globs
    tone_sample_count = config.persona.tone_sample_count
    tone_max_chars = config.persona.tone_max_chars_per_sample
    max_output_tokens = config.persona.max_output_tokens
    bio_globs = config.persona.bio_globs
    bio_include_full_text = config.persona.bio_include_full_text

    tone_items = _load_rawdata_from_globs(assets_folder, _coerce_str_list(tone_globs))
    if not tone_items:
        logger.warning("persona: no tone samples found (globs=%s)", tone_globs)

    bio_items = _load_rawdata_from_globs(assets_folder, _coerce_str_list(bio_globs))
    bio_text = ""
    if bio_items:
        if bio_include_full_text:
            bio_text = "\n\n---\n\n".join([x.content for x in bio_items])
        else:
            bio_text = "\n\n---\n\n".join([x.content[:2000] for x in bio_items])

    logger.info(
        "persona: generating (persona_name=%s tone_items=%d bio_items=%d)",
        persona_name,
        len(tone_items),
        len(bio_items),
    )

    generate_persona_profile(
        tone_items,
        output_path,
        core_cfg=core_cfg,
        persona_name=persona_name,
        bio_text=bio_text,
        sample_count=tone_sample_count,
        max_chars_per_sample=tone_max_chars,
        max_output_tokens=max_output_tokens,
    )

    return output_path


def _coerce_str_list(val: object) -> list[str]:
    """Coerce config glob values into a list of non-empty strings."""
    if is_object_list(val):
        out: list[str] = []
        for x in val:
            text = as_str(x).strip()
            if text:
                out.append(text)
        return out
    text = as_str(val).strip()
    if text:
        return [text]
    return []


def _iter_glob_paths(root: Path, patterns: Iterable[str]) -> list[Path]:
    """Collect unique glob matches under root."""
    out: list[Path] = []
    for pat in patterns:
        try:
            out.extend(list(root.glob(pat)))
        except Exception as e:
            logger.debug("Failed to glob pattern '%s': %s", pat, e)
            continue
    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        uniq.append(p)
    return uniq


def _load_rawdata_from_globs(root: Path, patterns: list[str]) -> list[RawData]:
    """Load RawData rows from JSONL files matched by glob patterns."""
    items: list[RawData] = []
    for p in _iter_glob_paths(root, patterns):
        if p.is_file() and p.suffix.lower() == ".jsonl":
            items.extend(read_raw_data_jsonl(p))
    return items
