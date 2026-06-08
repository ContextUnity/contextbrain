"""Taxonomy builder (CleanText -> taxonomy.json).

Design:
- Extract domain terms from samples using LLM
- Assign terms to predefined categories (from config) or let LLM create categories
- Predefined categories avoid mega-category problem
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TypedDict

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import (
    as_json_dict,
    as_json_dict_map,
    as_str,
    as_str_list,
    str_list_as_json,
)
from contextunity.core.parsing import json_loads
from contextunity.core.types import JsonDict, JsonValue, is_json_dict

from contextunity.brain.core import BrainConfig

from ..core.utils import (
    llm_generate_tsv,
    normalize_ambiguous_unicode,
    parallel_map,
    parse_tsv_line,
    strip_markdown_from_text,
)
from ..settings import RagIngestionConfig
from .taxonomy.sampling import (
    collect_clean_text_samples_from_dir,
)

logger = get_contextunit_logger(__name__)

_META_TERM_RE = re.compile(
    (
        r"\b(public domain|original text|author'?s|preface|foreword|appendix|"
        + r"toc|table of contents|chapter|page|copyright|isbn)\b"
    ),
    re.IGNORECASE,
)
_PROMO_TERM_RE = re.compile(
    r"\b(amazon|kindle|audible|youtube|subscribe|review|podcast)\b", re.IGNORECASE
)
_CONCEPT_PREFIX_RE = re.compile(r"^concepts\[\d+\](?:term)?:?\s*", re.IGNORECASE)


def build_taxonomy(
    source_root: Path,
    output_path: Path,
    config: RagIngestionConfig,
    core_cfg: BrainConfig,
    *,
    force_rebuild: bool = False,
    workers: int = 4,
) -> JsonDict:
    """Build taxonomy from CleanText samples."""
    existing: JsonDict | None = None
    if output_path.exists() and not force_rebuild:
        try:
            existing_wire = json_loads(output_path.read_text(encoding="utf-8"))
            existing = as_json_dict(existing_wire) if is_json_dict(existing_wire) else None
        except Exception as e:
            logger.warning("Failed to load existing taxonomy: %s", e)

    focus = config.taxonomy.philosophy_focus.strip()
    max_samples = config.taxonomy.max_samples
    scan_model = (
        config.taxonomy.scan_model.strip() or core_cfg.models.ingestion.taxonomy.model.strip()
    )

    # Load predefined categories from config
    predefined_cats = _load_predefined_categories(config.taxonomy.categories)

    samples = collect_clean_text_samples_from_dir(
        clean_text_dir=source_root, config=config, max_samples=max_samples
    )
    logger.info(
        "taxonomy: samples=%d scan_model=%s predefined_categories=%d",
        len(samples),
        scan_model,
        len(predefined_cats),
    )

    if not samples:
        return existing or _empty_taxonomy(focus)

    # Extract terms
    terms = _extract_terms(
        core_cfg=core_cfg,
        samples=[s.text for s in samples],
        focus=focus,
        model=scan_model,
        workers=workers,
    )
    if not terms:
        logger.warning("taxonomy: no terms extracted")
        return existing or _empty_taxonomy(focus)

    # Assign terms to categories
    if predefined_cats:
        assigned = _assign_terms_to_categories(core_cfg, terms, predefined_cats, model=scan_model)
    else:
        # Fallback: use LLM-assigned categories from extraction
        assigned = terms

    # Build taxonomy structure
    new_tax = _build_taxonomy_structure(assigned, predefined_cats, focus)

    if existing:
        new_tax = _merge_taxonomy(existing, new_tax)

    new_tax = _finalize_taxonomy(new_tax)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _ = output_path.write_text(json.dumps(new_tax, ensure_ascii=False, indent=2), encoding="utf-8")
    return new_tax


def _load_predefined_categories(cats: dict[str, str]) -> dict[str, str]:
    """Load predefined categories from config. Returns {name: description}."""
    return {
        _to_snake_case(k): str(v).strip() for k, v in cats.items() if k.strip() and str(v).strip()
    }


def _extract_terms(
    *,
    core_cfg: BrainConfig,
    samples: list[str],
    focus: str,
    model: str,
    workers: int,
) -> list[JsonDict]:
    """Extract domain terms from samples. No category assignment here."""
    batch_size = 10
    terms: list[JsonDict] = []

    prompts: list[str] = []
    for start in range(0, len(samples), batch_size):
        batch = samples[start : start + batch_size]
        combined = "\n\n---\n\n".join(batch)[:50000]

        prompt = f"""Extract domain concepts and terminology.

FOCUS: {focus}

CONTENT:
{combined}

Return TSV: term<TAB>synonyms<TAB>description
- Extract 15-25 high-signal concepts per batch
- term: 2-5 word phrase (domain-specific)
- synonyms: semicolon-separated (can be empty)
- description: one sentence
- No generic words, no proper names, no markdown
"""
        prompts.append(prompt)

    logger.info("taxonomy: extracting terms from %d batches", len(prompts))

    # temperature=0 for deterministic output
    if workers <= 1:
        for prompt in prompts:
            raw = llm_generate_tsv(
                core_cfg=core_cfg,
                prompt=prompt,
                model=model,
                temperature=0.0,
                max_tokens=4096,
                retries=3,
            )
            terms.extend(_parse_terms_tsv(raw))
    else:
        raws = parallel_map(
            prompts,
            lambda p: llm_generate_tsv(
                core_cfg=core_cfg,
                prompt=p,
                model=model,
                temperature=0.0,
                max_tokens=4096,
                retries=3,
            ),
            workers=workers,
            ordered=True,
            swallow_exceptions=True,
        )
        for raw in raws:
            terms.extend(_parse_terms_tsv(str(raw or "")))

    logger.info("taxonomy: extracted %d terms", len(terms))
    return terms


def _parse_terms_tsv(text: str) -> list[JsonDict]:
    """Parse TSV output into term dicts."""
    out: list[JsonDict] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.lower().startswith("term\t"):
            continue
        ln = _CONCEPT_PREFIX_RE.sub("", ln)

        parts = parse_tsv_line(ln)
        if len(parts) < 2:
            continue

        term = strip_markdown_from_text(normalize_ambiguous_unicode(parts[0].strip()))
        synonyms_s = normalize_ambiguous_unicode(parts[1].strip()) if len(parts) >= 2 else ""
        desc = normalize_ambiguous_unicode(parts[2].strip()) if len(parts) >= 3 else ""

        if not term or len(term) < 3 or len(term) > 80:
            continue
        # Normalize for word-boundary matching: treat underscores/punct as separators.
        term_lc = term.lower()
        term_lc_words = re.sub(r"[^a-z0-9]+", " ", term_lc).strip()

        # Reject obvious promo/meta/junk.
        if synonyms_s.strip().lower() in {"promo", "promotional", "advertisement", "marketing"}:
            continue
        if term.isupper() and len(term) <= 8:
            continue
        if _META_TERM_RE.search(term_lc_words) or _PROMO_TERM_RE.search(term_lc_words):
            continue
        if term.lower().startswith("concepts["):
            continue

        synonyms = [
            s.strip()
            for s in synonyms_s.split(";")
            if s.strip() and s.strip().lower() != term.lower()
        ]

        synonym_values: list[JsonValue] = []
        for syn in synonyms:
            synonym_values.append(syn)
        out.append(
            {
                "term": term,
                "synonyms": synonym_values,
                "description": desc,
                "category": "concepts",
            }
        )
    return out


def _assign_terms_to_categories(
    core_cfg: BrainConfig,
    terms: list[JsonDict],
    categories: dict[str, str],
    *,
    model: str,
    batch_size: int = 20,
) -> list[JsonDict]:
    """Assign each term to one of the predefined categories."""
    if not terms or not categories:
        return terms

    cat_names = list(categories.keys())
    cat_lines = "\n".join([f"- {name}: {desc}" for name, desc in categories.items()])

    term_list = [as_str(t.get("term")) for t in terms]
    term_map = {as_str(t.get("term")).lower(): t for t in terms}

    logger.info("taxonomy: assigning %d terms to %d categories", len(terms), len(categories))

    for start in range(0, len(term_list), batch_size):
        batch = term_list[start : start + batch_size]

        prompt = f"""Assign each term to exactly ONE category.

CATEGORIES:
{cat_lines}

TERMS:
{json.dumps(batch, ensure_ascii=False)}

Return TSV: term<TAB>category_snake_case
One line per term. Category must be one of: {", ".join(cat_names)}
"""
        raw = llm_generate_tsv(
            core_cfg=core_cfg,
            prompt=prompt,
            model=model,
            temperature=0.0,
            max_tokens=2048,
            retries=2,
        )

        for ln in (raw or "").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            parts = parse_tsv_line(ln)
            if len(parts) < 2:
                continue
            term_raw = parts[0].strip()
            cat_raw = _to_snake_case(parts[1].strip())

            if cat_raw not in cat_names:
                continue

            t = term_map.get(term_raw.lower())
            if t:
                t["category"] = cat_raw

    # Ensure all terms have a valid category
    default_cat = cat_names[0] if cat_names else "concepts"
    for t in terms:
        if t.get("category") not in cat_names:
            t["category"] = default_cat

    return terms


class _TaxonomyCategory(TypedDict):
    description: str
    keywords: list[str]
    synonyms: dict[str, list[str]]


def _category_to_json(cat: _TaxonomyCategory) -> JsonDict:
    synonyms_json: dict[str, JsonValue] = {}
    for term, values in cat["synonyms"].items():
        synonyms_json[term] = str_list_as_json(values)
    return {
        "description": cat["description"],
        "keywords": str_list_as_json(cat["keywords"]),
        "synonyms": synonyms_json,
    }


def _categories_to_json(cats: dict[str, _TaxonomyCategory]) -> dict[str, JsonValue]:
    return {name: _category_to_json(cat) for name, cat in cats.items()}


def _build_taxonomy_structure(
    terms: list[JsonDict],
    predefined_cats: dict[str, str],
    focus: str,
) -> JsonDict:
    """Build taxonomy dict from assigned terms."""
    cats: dict[str, _TaxonomyCategory] = {}

    # Initialize predefined categories
    for name, desc in predefined_cats.items():
        cats[name] = {"description": desc, "keywords": [], "synonyms": {}}

    # Add terms to categories
    for t in terms:
        term = as_str(t.get("term")).strip()
        cat = as_str(t.get("category"), default="concepts")
        if not term:
            continue

        if cat not in cats:
            cats[cat] = {
                "description": f"Concepts related to {cat.replace('_', ' ')}",
                "keywords": [],
                "synonyms": {},
            }

        cats[cat]["keywords"].append(term)

        syns = as_str_list(t.get("synonyms"))
        if syns:
            cats[cat]["synonyms"][term] = syns

    # Dedup keywords
    for cat_data in cats.values():
        seen: set[str] = set()
        uniq: list[str] = []
        for kw in cat_data["keywords"]:
            k = kw.lower()
            if k not in seen:
                seen.add(k)
                uniq.append(kw)
        cat_data["keywords"] = uniq

    # Remove empty categories
    cats = {k: v for k, v in cats.items() if v["keywords"]}

    return {"philosophy_focus": focus, "categories": _categories_to_json(cats)}


def _merge_taxonomy(existing: JsonDict, new: JsonDict) -> JsonDict:
    """Merge new taxonomy into existing."""
    focus = as_str(new.get("philosophy_focus"), default=as_str(existing.get("philosophy_focus")))
    ex_cats = as_json_dict_map(existing.get("categories"))
    new_cats = as_json_dict_map(new.get("categories"))
    merged_categories: dict[str, JsonValue] = {}

    all_cats = set(ex_cats.keys()) | set(new_cats.keys())
    for cat in all_cats:
        e = ex_cats.get(cat, {})
        n = new_cats.get(cat, {})

        # Merge keywords (new first, then existing)
        seen: set[str] = set()
        kws: list[str] = []
        for kw in as_str_list(n.get("keywords")) + as_str_list(e.get("keywords")):
            k = kw.strip().lower()
            if k and k not in seen:
                seen.add(k)
                kws.append(kw.strip())

        # Merge synonyms (string lists only)
        syns: dict[str, list[str]] = {}
        for src in (e, n):
            raw_syns = src.get("synonyms")
            if not isinstance(raw_syns, dict):
                continue
            for key, value in raw_syns.items():
                merged = syns.get(key, [])
                merged.extend(as_str_list(value))
                syns[key] = merged

        merged_categories[cat] = _category_to_json(
            {
                "description": as_str(n.get("description"), default=as_str(e.get("description"))),
                "keywords": kws,
                "synonyms": syns,
            }
        )

    return {"philosophy_focus": focus, "categories": merged_categories}


def _finalize_taxonomy(taxonomy: JsonDict) -> JsonDict:
    """Add all_keywords, canonical_map, total_count."""
    all_keywords: list[str] = []
    canonical_map: dict[str, str] = {}

    for cat_data in as_json_dict_map(taxonomy.get("categories")).values():
        for kw in as_str_list(cat_data.get("keywords")):
            if kw.strip():
                all_keywords.append(kw.strip())
        raw_syns = cat_data.get("synonyms")
        if not isinstance(raw_syns, dict):
            continue
        for canonical, syn_list in raw_syns.items():
            if not canonical.strip():
                continue
            canonical_map[canonical.strip().lower()] = canonical.strip()
            for s in as_str_list(syn_list):
                if s.strip():
                    canonical_map[s.strip().lower()] = canonical.strip()

    # Dedup
    seen: set[str] = set()
    uniq: list[str] = []
    for kw in all_keywords:
        k = kw.lower()
        if k not in seen:
            seen.add(k)
            uniq.append(kw)

    taxonomy["all_keywords"] = str_list_as_json(uniq)
    taxonomy["canonical_map"] = dict(canonical_map)
    taxonomy["total_count"] = len(uniq)
    return taxonomy


def _empty_taxonomy(focus: str) -> JsonDict:
    return {
        "philosophy_focus": focus,
        "categories": {},
        "all_keywords": [],
        "canonical_map": {},
        "total_count": 0,
    }


def _to_snake_case(val: str) -> str:
    s = (val or "").strip()
    if not s:
        return ""
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"[^a-zA-Z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s)
    return s.lower().strip("_")
