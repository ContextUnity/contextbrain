"""Tests for ingestion utility functions.

Zero-mock tests for pure text processing, enrichment, and parallel utilities.
"""

from __future__ import annotations

import pytest

from contextunity.brain.ingestion.rag.core.utils import (
    build_enriched_input_text,
    clean_markdown_headers,
    clean_str_list,
    filter_testimonial_signatures,
    get_graph_enrichment,
    normalize_ambiguous_unicode,
    normalize_clean_text,
    parallel_map,
    parse_tsv_line,
    strip_markdown_from_text,
)

# ═══════════════════════════════════════════════════════════════════
# parse_tsv_line
# ═══════════════════════════════════════════════════════════════════


class TestParseTsvLine:
    """Split TSV lines — real tabs or <TAB> markers."""

    def test_real_tab(self):
        assert parse_tsv_line("a\tb\tc") == ["a", "b", "c"]

    def test_tab_marker(self):
        assert parse_tsv_line("a<TAB>b<TAB>c") == ["a", "b", "c"]

    def test_no_separator_returns_empty(self):
        assert parse_tsv_line("no tabs here") == []

    def test_empty_string(self):
        assert parse_tsv_line("") == []

    def test_real_tab_preferred_over_marker(self):
        result = parse_tsv_line("a\tb<TAB>c")
        assert result == ["a", "b<TAB>c"]  # splits on real tab first


# ═══════════════════════════════════════════════════════════════════
# normalize_ambiguous_unicode
# ═══════════════════════════════════════════════════════════════════


class TestNormalizeAmbiguousUnicode:
    """Unicode → ASCII normalization."""

    def test_em_dash(self):
        assert normalize_ambiguous_unicode("hello\u2014world") == "hello--world"

    def test_en_dash(self):
        assert normalize_ambiguous_unicode("2020\u20132025") == "2020-2025"

    def test_smart_quotes(self):
        assert normalize_ambiguous_unicode("\u201cHello\u201d") == '"Hello"'

    def test_ellipsis(self):
        assert normalize_ambiguous_unicode("wait\u2026") == "wait..."

    def test_bullet(self):
        assert normalize_ambiguous_unicode("\u2022 item") == "* item"

    def test_fullwidth_colon(self):
        assert normalize_ambiguous_unicode("Title\uff1a Value") == "Title: Value"

    def test_no_change_for_ascii(self):
        text = "plain ASCII text 123"
        assert normalize_ambiguous_unicode(text) == text

    def test_non_breaking_space(self):
        assert normalize_ambiguous_unicode("hello\u00a0world") == "hello world"


# ═══════════════════════════════════════════════════════════════════
# normalize_clean_text
# ═══════════════════════════════════════════════════════════════════


class TestNormalizeCleanText:
    """HTML unescape + whitespace normalization."""

    def test_html_entity(self):
        assert normalize_clean_text("it&#39;s") == "it's"

    def test_newlines_replaced(self):
        assert normalize_clean_text("line1\nline2") == "line1 line2"

    def test_extra_spaces_collapsed(self):
        assert normalize_clean_text("  too   many   spaces  ") == "too many spaces"

    def test_combined(self):
        assert normalize_clean_text("  hello\n  &amp; world  ") == "hello & world"


# ═══════════════════════════════════════════════════════════════════
# clean_markdown_headers
# ═══════════════════════════════════════════════════════════════════


class TestCleanMarkdownHeaders:
    """Normalize markdown header formatting."""

    def test_extra_spaces_stripped(self):
        assert clean_markdown_headers("###   HEADER   ") == "### HEADER"

    def test_preserves_non_headers(self):
        text = "Regular paragraph"
        assert clean_markdown_headers(text) == text

    def test_multiline(self):
        text = "## Chapter 1  \nSome content\n###  Section  "
        result = clean_markdown_headers(text)
        assert "## Chapter 1" in result
        assert "### Section" in result
        assert "Some content" in result


# ═══════════════════════════════════════════════════════════════════
# strip_markdown_from_text
# ═══════════════════════════════════════════════════════════════════


class TestStripMarkdown:
    """Remove markdown formatting, keep text."""

    def test_headers_removed(self):
        assert strip_markdown_from_text("## Chapter 1") == "Chapter 1"

    def test_bold_removed(self):
        assert strip_markdown_from_text("**Bold text**") == "Bold text"

    def test_italic_removed(self):
        assert strip_markdown_from_text("_italic_") == "italic"

    def test_links_text_kept(self):
        assert strip_markdown_from_text("[click here](https://example.com)") == "click here"

    def test_combined(self):
        text = "## Title\n**Bold** and _italic_ with [link](url)"
        result = strip_markdown_from_text(text)
        assert "#" not in result
        assert "**" not in result
        assert "_" not in result
        assert "Bold" in result
        assert "italic" in result
        assert "link" in result


# ═══════════════════════════════════════════════════════════════════
# filter_testimonial_signatures
# ═══════════════════════════════════════════════════════════════════


class TestFilterTestimonialSignatures:
    """Filter out testimonial lines from book content."""

    def test_em_dash_signature_kept_as_content(self):
        result = filter_testimonial_signatures("— John Doe")
        assert "— John Doe" in result

    def test_double_dash_kept(self):
        result = filter_testimonial_signatures("-- Author Name")
        assert "-- Author Name" in result

    def test_long_dash_item_not_filtered(self):
        """Lines starting with '- ' longer than 100 chars are NOT filtered."""
        long_line = "- " + "x" * 200
        result = filter_testimonial_signatures(long_line)
        assert long_line in result

    def test_normal_content_untouched(self):
        text = "Normal paragraph\nAnother line"
        assert filter_testimonial_signatures(text) == text


# ═══════════════════════════════════════════════════════════════════
# parallel_map
# ═══════════════════════════════════════════════════════════════════


class TestParallelMap:
    """Bounded parallel execution."""

    def test_empty_input(self):
        assert parallel_map([], lambda x: x, workers=4) == []

    def test_single_worker_sequential(self):
        result = parallel_map([1, 2, 3], lambda x: x * 2, workers=1)
        assert result == [2, 4, 6]

    def test_multi_worker(self):
        result = parallel_map([1, 2, 3], lambda x: x * 2, workers=2, ordered=True)
        assert result == [2, 4, 6]

    def test_ordered_preserves_positions(self):
        result = parallel_map([10, 20, 30], lambda x: x + 1, workers=3, ordered=True)
        assert result == [11, 21, 31]

    def test_exception_swallowed(self):
        def boom(x):
            if x == 2:
                raise ValueError("boom")
            return x

        result = parallel_map([1, 2, 3], boom, workers=1, swallow_exceptions=True)
        assert result == [1, None, 3]

    def test_exception_propagated(self):
        def boom(x):
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            parallel_map([1], boom, workers=1, swallow_exceptions=False)

    def test_unordered_multi_worker(self):
        result = parallel_map([1, 2, 3], lambda x: x, workers=3, ordered=False)
        assert sorted(result) == [1, 2, 3]

    def test_default_ordered_is_false(self):
        """Default ordered=False — results may arrive out of order."""
        result = parallel_map([1, 2, 3], lambda x: x, workers=2)
        assert sorted(result) == [1, 2, 3]

    def test_default_swallow_exceptions_is_true(self):
        """Default swallow_exceptions=True — errors produce None, not crash."""

        def boom(x):
            if x == 2:
                raise ValueError("boom")
            return x

        result = parallel_map([1, 2, 3], boom, workers=1)
        assert result == [1, None, 3]


# ═══════════════════════════════════════════════════════════════════
# build_enriched_input_text
# ═══════════════════════════════════════════════════════════════════


class TestBuildEnrichedInputText:
    """Search payload construction with enrichment."""

    def test_content_only(self):
        result = build_enriched_input_text(content="Hello")
        assert result == "Hello"

    def test_with_categories(self):
        result = build_enriched_input_text(content="X", parent_categories=["Science", "Physics"])
        assert "Categories: Science, Physics" in result

    def test_categories_limited_to_5(self):
        cats = [f"cat{i}" for i in range(10)]
        result = build_enriched_input_text(content="X", parent_categories=cats)
        assert "cat5" not in result

    def test_with_summary(self):
        result = build_enriched_input_text(content="X", summary="A brief overview")
        assert "Additional Knowledge: A brief overview" in result

    def test_single_keyword(self):
        result = build_enriched_input_text(content="X", keywords=["quantum"])
        assert "related to quantum" in result

    def test_two_keywords(self):
        result = build_enriched_input_text(content="X", keywords=["AI", "ML"])
        assert "AI and ML" in result

    def test_many_keywords(self):
        result = build_enriched_input_text(content="X", keywords=["a", "b", "c", "d", "e"])
        assert "and other concepts" in result

    def test_empty_keyword_list(self):
        result = build_enriched_input_text(content="X", keywords=[])
        assert result == "X"


# ═══════════════════════════════════════════════════════════════════
# get_graph_enrichment
# ═══════════════════════════════════════════════════════════════════


class TestGetGraphEnrichment:
    """Normalize GraphEnricher output."""

    def test_normal_enrichment(self):
        def enrich(text):
            return {"keywords": ["k1", "k2"], "summary": "Sum", "parent_categories": ["cat1"]}

        kw, summ, cats = get_graph_enrichment(text="X", enrichment_func=enrich)
        assert kw == ["k1", "k2"]
        assert summ == "Sum"
        assert cats == ["cat1"]

    def test_exception_returns_defaults(self):
        def boom(text):
            raise RuntimeError("fail")

        kw, summ, cats = get_graph_enrichment(text="X", enrichment_func=boom)
        assert kw == []
        assert summ == ""
        assert cats == []

    def test_non_dict_returns_defaults(self):
        kw, summ, cats = get_graph_enrichment(text="X", enrichment_func=lambda x: "not a dict")
        assert kw == []

    def test_keywords_capped_at_50(self):
        def enrich(text):
            return {
                "keywords": [f"k{i}" for i in range(100)],
                "summary": "",
                "parent_categories": [],
            }

        kw, _, _ = get_graph_enrichment(text="X", enrichment_func=enrich)
        assert len(kw) == 50

    def test_empty_strings_filtered(self):
        def enrich(text):
            return {
                "keywords": ["", "  ", "valid"],
                "summary": "  ",
                "parent_categories": ["", "cat"],
            }

        kw, summ, cats = get_graph_enrichment(text="X", enrichment_func=enrich)
        assert kw == ["valid"]
        assert summ == ""
        assert cats == ["cat"]


# ═══════════════════════════════════════════════════════════════════
# clean_str_list
# ═══════════════════════════════════════════════════════════════════


class TestCleanStrList:
    """String list normalization with dedup."""

    def test_basic(self):
        assert clean_str_list(["a", "b", "c"], limit=10) == ["a", "b", "c"]

    def test_dedup_case_insensitive(self):
        assert clean_str_list(["Hello", "hello", "HELLO"], limit=10) == ["Hello"]

    def test_dedup_case_sensitive(self):
        result = clean_str_list(["Hello", "hello"], limit=10, dedupe_case_insensitive=False)
        assert result == ["Hello", "hello"]

    def test_limit_enforced(self):
        assert len(clean_str_list(["a", "b", "c", "d"], limit=2)) == 2

    def test_empty_strings_filtered(self):
        assert clean_str_list(["", "  ", "valid"], limit=10) == ["valid"]

    def test_non_strings_filtered(self):
        assert clean_str_list([1, None, "ok"], limit=10) == ["ok"]

    def test_none_input(self):
        assert clean_str_list(None, limit=5) == []

    def test_zero_limit_still_yields_one(self):
        """limit=0 is edge case — breaks after first append due to `>= max(0, 0)`."""
        assert clean_str_list(["a", "b"], limit=0) == ["a"]
