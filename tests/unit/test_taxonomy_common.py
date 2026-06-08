"""Tests for taxonomy sampling common helpers.

Zero-mock tests for deterministic hashing, text cleaning, and windowed
snippet extraction used in taxonomy sampling pipeline.
"""

from __future__ import annotations

from contextunity.brain.ingestion.rag.processors.taxonomy.common import (
    clean_for_taxonomy_sample,
    stable_hash_u64,
    windowed_snippets,
)

# ═══════════════════════════════════════════════════════════════════
# stable_hash_u64
# ═══════════════════════════════════════════════════════════════════


class TestStableHash:
    """Deterministic hash for sampling order."""

    def test_deterministic(self):
        assert stable_hash_u64("hello") == stable_hash_u64("hello")

    def test_different_inputs(self):
        assert stable_hash_u64("hello") != stable_hash_u64("world")

    def test_returns_positive_int(self):
        h = stable_hash_u64("test")
        assert isinstance(h, int)
        assert h >= 0

    def test_empty_string(self):
        h = stable_hash_u64("")
        assert isinstance(h, int)


# ═══════════════════════════════════════════════════════════════════
# clean_for_taxonomy_sample
# ═══════════════════════════════════════════════════════════════════


class TestCleanForTaxonomy:
    """Text cleaning for taxonomy input."""

    def test_strips_page_markers(self):
        text = "Some text <page: 42> more text"
        result = clean_for_taxonomy_sample(text)
        assert "<page" not in result
        assert "Some text" in result
        assert "more text" in result

    def test_strips_end_of_page(self):
        text = "Content --- end of page 5 --- after"
        result = clean_for_taxonomy_sample(text)
        assert "end of page" not in result

    def test_collapses_whitespace(self):
        text = "  too   many    spaces  "
        result = clean_for_taxonomy_sample(text)
        assert "  " not in result
        assert result == result.strip()

    def test_normalizes_unicode(self):
        """Smart quotes and em-dashes replaced."""
        text = "\u201cHello\u201d \u2014 world"
        result = clean_for_taxonomy_sample(text)
        assert "\u201c" not in result
        assert "\u2014" not in result

    def test_empty_input(self):
        assert clean_for_taxonomy_sample("") == ""
        assert clean_for_taxonomy_sample(None) == ""


# ═══════════════════════════════════════════════════════════════════
# windowed_snippets
# ═══════════════════════════════════════════════════════════════════


class TestWindowedSnippets:
    """Evenly-spaced text windows for sampling."""

    def test_short_text_single_window(self):
        result = windowed_snippets("hello world", window_chars=100, max_windows=3)
        assert result == ["hello world"]

    def test_empty_text(self):
        assert windowed_snippets("", window_chars=100, max_windows=3) == []
        assert windowed_snippets("   ", window_chars=100, max_windows=3) == []

    def test_none_text(self):
        assert windowed_snippets(None, window_chars=100, max_windows=3) == []

    def test_zero_window_chars_returns_full(self):
        assert windowed_snippets("hello", window_chars=0, max_windows=1) == ["hello"]

    def test_max_windows_1_returns_prefix(self):
        text = "a" * 200
        result = windowed_snippets(text, window_chars=50, max_windows=1)
        assert len(result) == 1
        assert len(result[0]) == 50

    def test_multiple_windows_evenly_spaced(self):
        # Use diverse text so dedup doesn't collapse windows
        text = "".join(chr(65 + (i % 26)) for i in range(1000))
        result = windowed_snippets(text, window_chars=100, max_windows=5)
        assert len(result) >= 2  # at least 2 distinct windows
        assert all(len(w) <= 100 for w in result)

    def test_dedup_overlapping_windows(self):
        """Short text with many windows requested — dedup avoids duplicates."""
        text = "a" * 150
        result = windowed_snippets(text, window_chars=100, max_windows=10)
        # Windows overlap heavily → dedup reduces count
        assert len(result) <= 10
        assert len(set(w[:200].lower() for w in result)) == len(result)
