"""Unit tests for BrainCell source-type confidence caps."""

from __future__ import annotations

from contextunity.brain.cell_confidence import cap_confidence


class TestCapConfidence:
    def test_manual_allows_high_confidence(self):
        assert cap_confidence("manual", 0.99) == 0.99

    def test_auto_extract_caps_at_medium(self):
        assert cap_confidence("auto_extract", 0.99) == 0.75

    def test_synthesis_caps_at_medium(self):
        assert cap_confidence("synthesis", 1.0) == 0.75

    def test_documentation_caps_low(self):
        assert cap_confidence("documentation", 0.9) == 0.5

    def test_unknown_source_uses_default_cap(self):
        assert cap_confidence("memory", 0.99) == 0.75
