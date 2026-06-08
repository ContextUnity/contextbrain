"""Tests for taxonomy builder pure functions.

Zero-mock tests for TSV parsing, taxonomy structure building, merging,
finalization, and helper utilities.
"""

from __future__ import annotations

from contextunity.core.narrowing import as_int

from contextunity.brain.ingestion.rag.processors.taxonomy_builder import (
    _build_taxonomy_structure,
    _finalize_taxonomy,
    _load_predefined_categories,
    _merge_taxonomy,
    _parse_terms_tsv,
    _to_snake_case,
)

# ═══════════════════════════════════════════════════════════════════
# _to_snake_case
# ═══════════════════════════════════════════════════════════════════


class TestToSnakeCase:
    def test_basic(self):
        assert _to_snake_case("Hello World") == "hello_world"

    def test_hyphens(self):
        assert _to_snake_case("my-category") == "my_category"

    def test_special_chars_stripped(self):
        assert _to_snake_case("cat!@#123") == "cat123"

    def test_empty_string(self):
        assert _to_snake_case("") == ""

    def test_leading_trailing_underscores(self):
        assert _to_snake_case("__foo__") == "foo"

    def test_multiple_underscores_collapsed(self):
        assert _to_snake_case("a   b   c") == "a_b_c"


# ═══════════════════════════════════════════════════════════════════
# as_int (taxonomy int coercion)
# ═══════════════════════════════════════════════════════════════════


class TestAsInt:
    def test_valid_int(self):
        assert as_int(42, default=0) == 42

    def test_valid_string(self):
        assert as_int("10", default=0) == 10

    def test_invalid_string(self):
        assert as_int("abc", default=99) == 99

    def test_none(self):
        assert as_int(None, default=5) == 5


# ═══════════════════════════════════════════════════════════════════
# _parse_terms_tsv
# ═══════════════════════════════════════════════════════════════════


class TestParseTermsTsv:
    """Parse LLM-generated TSV into term dicts."""

    def test_basic_tsv(self):
        tsv = "domain modeling\tDDD;data modeling\tFundamental technique"
        result = _parse_terms_tsv(tsv)
        assert len(result) == 1
        assert result[0]["term"] == "domain modeling"
        assert result[0]["synonyms"] == ["DDD", "data modeling"]
        assert result[0]["description"] == "Fundamental technique"

    def test_skips_header_line(self):
        tsv = "term\tsynonyms\tdescription\nreal term\tsyns\tdesc"
        result = _parse_terms_tsv(tsv)
        assert len(result) == 1
        assert result[0]["term"] == "real term"

    def test_rejects_too_short_term(self):
        tsv = "ab\t\tshort"
        assert _parse_terms_tsv(tsv) == []

    def test_rejects_too_long_term(self):
        long = "x" * 81
        tsv = f"{long}\t\tlong"
        assert _parse_terms_tsv(tsv) == []

    def test_rejects_promo_synonym(self):
        tsv = "some term\tpromo\tproduct description"
        assert _parse_terms_tsv(tsv) == []

    def test_rejects_meta_terms(self):
        tsv = "table of contents\t\tmeta"
        assert _parse_terms_tsv(tsv) == []

    def test_rejects_promo_terms(self):
        tsv = "subscribe now\t\tpromo"
        assert _parse_terms_tsv(tsv) == []

    def test_rejects_all_caps_short(self):
        """Short ALL-CAPS terms are acronyms/junk."""
        tsv = "ISBN\t\tidentifier"
        assert _parse_terms_tsv(tsv) == []

    def test_accepts_mixed_case(self):
        tsv = "Neural Networks\tANN;deep learning\tML technique"
        result = _parse_terms_tsv(tsv)
        assert len(result) == 1

    def test_empty_input(self):
        assert _parse_terms_tsv("") == []
        assert _parse_terms_tsv(None) == []

    def test_self_synonym_excluded(self):
        """Synonym matching the term itself is excluded."""
        tsv = "machine learning\tmachine learning;ML\tdescription"
        result = _parse_terms_tsv(tsv)
        assert result[0]["synonyms"] == ["ML"]

    def test_strips_concept_prefix(self):
        """concepts[0]term: prefix is removed."""
        tsv = "concepts[0]term: real concept\tsyns\tdesc"
        result = _parse_terms_tsv(tsv)
        assert len(result) == 1
        assert result[0]["term"] == "real concept"


# ═══════════════════════════════════════════════════════════════════
# _load_predefined_categories
# ═══════════════════════════════════════════════════════════════════


class TestLoadPredefinedCategories:
    def test_basic(self):
        result = _load_predefined_categories(
            {"Machine Learning": "ML and AI", "Data Engineering": "Pipelines and ETL"}
        )
        assert "machine_learning" in result
        assert "data_engineering" in result

    def test_empty_values_filtered(self):
        result = _load_predefined_categories({"valid": "desc", "bad": "", "": "no key"})
        assert result == {"valid": "desc"}


# ═══════════════════════════════════════════════════════════════════
# _build_taxonomy_structure
# ═══════════════════════════════════════════════════════════════════


class TestBuildTaxonomyStructure:
    def test_basic_structure(self):
        terms = [
            {"term": "gradient descent", "category": "optimization", "synonyms": ["GD"]},
            {"term": "backpropagation", "category": "optimization", "synonyms": []},
        ]
        result = _build_taxonomy_structure(terms, {"optimization": "Optimization methods"}, "ML")
        assert result["philosophy_focus"] == "ML"
        assert "optimization" in result["categories"]
        assert "gradient descent" in result["categories"]["optimization"]["keywords"]
        assert "backpropagation" in result["categories"]["optimization"]["keywords"]
        assert result["categories"]["optimization"]["synonyms"]["gradient descent"] == ["GD"]

    def test_dedup_keywords(self):
        terms = [
            {"term": "Tensor", "category": "math", "synonyms": []},
            {"term": "tensor", "category": "math", "synonyms": []},
        ]
        result = _build_taxonomy_structure(terms, {}, "")
        assert len(result["categories"]["math"]["keywords"]) == 1

    def test_empty_categories_removed(self):
        terms = [{"term": "term1", "category": "active", "synonyms": []}]
        result = _build_taxonomy_structure(terms, {"active": "has terms", "empty": "no terms"}, "")
        assert "active" in result["categories"]
        assert "empty" not in result["categories"]


# ═══════════════════════════════════════════════════════════════════
# _merge_taxonomy
# ═══════════════════════════════════════════════════════════════════


class TestMergeTaxonomy:
    def test_merge_keywords(self):
        existing = {
            "philosophy_focus": "old",
            "categories": {
                "cat1": {"description": "old desc", "keywords": ["kw1"], "synonyms": {}}
            },
        }
        new = {
            "philosophy_focus": "new",
            "categories": {
                "cat1": {"description": "new desc", "keywords": ["kw2"], "synonyms": {}}
            },
        }
        result = _merge_taxonomy(existing, new)
        assert result["philosophy_focus"] == "new"
        assert "kw1" in result["categories"]["cat1"]["keywords"]
        assert "kw2" in result["categories"]["cat1"]["keywords"]

    def test_merge_dedup(self):
        existing = {
            "categories": {"c": {"keywords": ["same"], "synonyms": {}, "description": ""}},
        }
        new = {
            "categories": {"c": {"keywords": ["Same"], "synonyms": {}, "description": "d"}},
        }
        result = _merge_taxonomy(existing, new)
        assert len(result["categories"]["c"]["keywords"]) == 1

    def test_merge_new_category(self):
        existing = {"categories": {}}
        new = {"categories": {"new_cat": {"keywords": ["k1"], "synonyms": {}, "description": "d"}}}
        result = _merge_taxonomy(existing, new)
        assert "new_cat" in result["categories"]


# ═══════════════════════════════════════════════════════════════════
# _finalize_taxonomy
# ═══════════════════════════════════════════════════════════════════


class TestFinalizeTaxonomy:
    def test_adds_all_keywords(self):
        tax = {
            "categories": {
                "c1": {"keywords": ["kw1", "kw2"], "synonyms": {}},
                "c2": {"keywords": ["kw3"], "synonyms": {}},
            }
        }
        result = _finalize_taxonomy(tax)
        assert set(result["all_keywords"]) == {"kw1", "kw2", "kw3"}
        assert result["total_count"] == 3

    def test_builds_canonical_map(self):
        tax = {
            "categories": {
                "c1": {"keywords": ["Neural Net"], "synonyms": {"Neural Net": ["ANN", "NN"]}}
            }
        }
        result = _finalize_taxonomy(tax)
        assert result["canonical_map"]["ann"] == "Neural Net"
        assert result["canonical_map"]["nn"] == "Neural Net"
        assert result["canonical_map"]["neural net"] == "Neural Net"

    def test_dedup_all_keywords(self):
        tax = {
            "categories": {
                "c1": {"keywords": ["Same"], "synonyms": {}},
                "c2": {"keywords": ["same"], "synonyms": {}},
            }
        }
        result = _finalize_taxonomy(tax)
        assert result["total_count"] == 1
