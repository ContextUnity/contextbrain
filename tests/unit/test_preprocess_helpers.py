"""Tests for preprocess stage pure functions.

Zero-mock tests for deterministic corrections and other extractable logic.
"""

from __future__ import annotations

from contextunity.brain.ingestion.rag.stages.preprocess import (
    _apply_deterministic_corrections,
)


class TestApplyDeterministicCorrections:
    """Exact substring replacement for transcript corrections."""

    def test_basic_replacement(self):
        text = "Welcome to Jhon's channel"
        corrections = {"Jhon": "John"}
        assert _apply_deterministic_corrections(text, corrections) == "Welcome to John's channel"

    def test_multiple_corrections(self):
        text = "Jhon said helo to everone"
        corrections = {"Jhon": "John", "helo": "hello", "everone": "everyone"}
        result = _apply_deterministic_corrections(text, corrections)
        assert result == "John said hello to everyone"

    def test_no_match_unchanged(self):
        text = "No corrections needed"
        assert _apply_deterministic_corrections(text, {"xyz": "abc"}) == text

    def test_empty_corrections(self):
        text = "Original text"
        assert _apply_deterministic_corrections(text, {}) == text

    def test_empty_text(self):
        assert _apply_deterministic_corrections("", {"a": "b"}) == ""

    def test_multiple_occurrences(self):
        text = "foo bar foo baz foo"
        assert _apply_deterministic_corrections(text, {"foo": "qux"}) == "qux bar qux baz qux"

    def test_case_sensitive(self):
        """Corrections are case-sensitive (exact substring)."""
        text = "Hello hello HELLO"
        result = _apply_deterministic_corrections(text, {"hello": "world"})
        assert result == "Hello world HELLO"

    def test_empty_key_skipped(self):
        """Empty string key should not cause infinite loop."""
        text = "Some text"
        result = _apply_deterministic_corrections(text, {"": "bad"})
        assert result == text
