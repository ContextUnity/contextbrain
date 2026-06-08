"""Enrichment stage: add NER + keyphrases metadata to CleanText."""

from __future__ import annotations

import asyncio
import time

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import (
    as_json_dict,
    as_str,
    as_str_list,
    json_dict_list_as_json,
    str_list_as_json,
)
from contextunity.core.types import JsonDict, is_json_dict, is_object_list

from contextunity.brain.core import BrainConfig
from contextunity.brain.ingestion.rag.config import get_assets_paths
from contextunity.brain.ingestion.rag.core.types import RawData
from contextunity.brain.ingestion.rag.core.utils import resolve_workers
from contextunity.brain.ingestion.rag.settings import RagIngestionConfig
from contextunity.brain.ingestion.rag.stages.store import (
    read_raw_data_jsonl,
    write_raw_data_jsonl,
)
from contextunity.brain.modules.intelligence.keyphrases import KeyphraseExtractor
from contextunity.brain.modules.intelligence.ner import EntityExtractor

logger = get_contextunit_logger(__name__)


def _merge_keywords(existing: object, new: object) -> list[str]:
    """Merge keyword lists preserving order and case-insensitive uniqueness."""
    base = as_str_list(existing) if is_object_list(existing) else []
    add = as_str_list(new) if is_object_list(new) else []
    merged: list[str] = []
    seen: set[str] = set()
    for item in base + add:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


async def enrich_clean_text(
    *,
    config: RagIngestionConfig,
    core_cfg: BrainConfig,
    only_types: list[str],
    overwrite: bool = True,
    workers: int = 1,
) -> dict[str, str]:
    """Enrich clean text."""
    _ = core_cfg
    if not config.enrichment.ner_enabled and not config.enrichment.keyphrases_enabled:
        logger.info("enrich: disabled (enrichment.ner_enabled/keyphrases_enabled=false)")
        return {}

    if not overwrite:
        logger.warning("enrich: overwrite=false, skipping (would rewrite clean_text files)")
        return {}

    paths = get_assets_paths(config)
    out_paths: dict[str, str] = {}

    async def _enrich_items(items: list[RawData]) -> list[RawData]:
        ner: EntityExtractor | None = None
        if config.enrichment.ner_enabled:
            ner = EntityExtractor(default_mode=config.enrichment.ner.mode)

        keyphrases: KeyphraseExtractor | None = None
        if config.enrichment.keyphrases_enabled:
            keyphrases = KeyphraseExtractor()

        enriched: list[RawData] = []
        for item in items:
            metadata: JsonDict = as_json_dict(item.metadata)
            try:
                if ner:
                    entities = await ner.extract(item.content, mode=config.enrichment.ner.mode)
                    if entities:
                        entity_rows: list[JsonDict] = [
                            {
                                "text": ent.text,
                                "label": ent.label,
                                "confidence": ent.confidence,
                            }
                            for ent in entities
                        ]
                        metadata["ner_entities"] = json_dict_list_as_json(entity_rows)
                        metadata["ner_entity_count"] = len(entities)
                        by_type: dict[str, list[JsonDict]] = {}
                        for ent in entities:
                            by_type.setdefault(ent.label, []).append(
                                {"text": ent.text, "confidence": ent.confidence}
                            )
                        metadata["ner_entities_by_type"] = {
                            label: json_dict_list_as_json(rows) for label, rows in by_type.items()
                        }
                if keyphrases:
                    phrases = keyphrases.extract(
                        item.content, limit=config.enrichment.keyphrases.max_phrases
                    )
                    metadata["keyphrase_texts"] = str_list_as_json(phrases)
            except Exception:
                logger.exception("enrich: failed to enrich item, keeping original metadata")

            ner_entities_obj = metadata.get("ner_entities")
            if is_object_list(ner_entities_obj):
                ner_texts: list[str] = []
                for ent in ner_entities_obj:
                    if is_json_dict(ent):
                        text = as_str(ent.get("text")).strip()
                        if text:
                            ner_texts.append(text)
                metadata["keywords"] = str_list_as_json(
                    _merge_keywords(metadata.get("keywords"), ner_texts)
                )
            if keyphrases:
                metadata["keywords"] = str_list_as_json(
                    _merge_keywords(metadata.get("keywords"), metadata.get("keyphrase_texts"))
                )
            item.metadata = metadata
            enriched.append(item)
        return enriched

    async def _run_one(t: str, sem: asyncio.Semaphore) -> tuple[str, str]:
        async with sem:
            t0 = time.perf_counter()
            in_path = paths["clean_text"] / f"{t}.jsonl"

            try:
                items = await asyncio.to_thread(read_raw_data_jsonl, in_path)
            except Exception:
                logger.warning("enrich: failed to read %s", in_path)
                return (t, "")

            if not items:
                logger.warning("enrich: no clean_text items for type=%s at %s", t, in_path)
                return (t, "")

            enriched = await _enrich_items(items)

            out_path = paths["clean_text"] / f"{t}.jsonl"

            count = await asyncio.to_thread(
                write_raw_data_jsonl, enriched, out_path, overwrite=True
            )

            logger.warning(
                "enrich: wrote %d records for type=%s -> %s (%.1fs)",
                count,
                t,
                out_path,
                time.perf_counter() - t0,
            )
            return (t, str(out_path))

    w = resolve_workers(config=config, workers=workers)
    sem = asyncio.Semaphore(w)

    tasks = [_run_one(t, sem) for t in only_types]
    results = await asyncio.gather(*tasks)

    for r in results:
        if not r:
            continue
        tt, out_path = r
        if out_path:
            out_paths[tt] = out_path

    return out_paths


__all__ = ["enrich_clean_text"]
