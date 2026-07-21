"""Tests for NLP enrichment data classes and math utilities.

Zero-mock tests for EnrichmentResult methods and cosine similarity.
Extractor/classifier classes require ML models — integration scope.
"""

from __future__ import annotations

from inspect import signature

import pytest

from contextunity.brain.modules.intelligence.ner import EntityExtractor
from contextunity.brain.service.nlp import (
    EnrichmentResult,
    Entity,
    ZeroShotClassifier,
)

# ═══════════════════════════════════════════════════════════════════
# EnrichmentResult.entity_map
# ═══════════════════════════════════════════════════════════════════


class TestEntityMap:
    """Group entities by label."""

    def test_groups_by_label(self):
        result = EnrichmentResult(
            entities=[
                Entity(text="Kyiv", label="GPE"),
                Entity(text="Apple", label="ORG"),
                Entity(text="London", label="GPE"),
            ]
        )
        em = result.entity_map
        assert em["GPE"] == ["Kyiv", "London"]
        assert em["ORG"] == ["Apple"]

    def test_empty_entities(self):
        assert EnrichmentResult().entity_map == {}

    def test_single_entity(self):
        result = EnrichmentResult(entities=[Entity(text="Bob", label="PERSON")])
        assert result.entity_map == {"PERSON": ["Bob"]}


# ═══════════════════════════════════════════════════════════════════
# EnrichmentResult.top_category
# ═══════════════════════════════════════════════════════════════════


class TestTopCategory:
    def test_returns_highest_scoring(self):
        result = EnrichmentResult(categories=[("commerce", 0.9), ("infra", 0.5)])
        assert result.top_category == "commerce"

    def test_empty_returns_none(self):
        assert EnrichmentResult().top_category is None


# ═══════════════════════════════════════════════════════════════════
# EnrichmentResult.to_metadata
# ═══════════════════════════════════════════════════════════════════


class TestToMetadata:
    def test_full_enrichment(self):
        result = EnrichmentResult(
            entities=[Entity(text="Kyiv", label="GPE")],
            topics=["vector search", "embeddings"],
            categories=[("commerce", 0.85), ("infra", 0.3)],
            language="en",
        )
        meta = result.to_metadata()
        assert meta["topics"] == ["vector search", "embeddings"]
        assert meta["category"] == "commerce"
        assert meta["language"] == "en"
        assert "entities_GPE" in meta
        assert "Kyiv" in meta["entities_GPE"]
        assert meta["category_scores"]["commerce"] == 0.85

    def test_empty_enrichment(self):
        assert EnrichmentResult().to_metadata() == {}

    def test_deduplicates_entity_values(self):
        result = EnrichmentResult(
            entities=[
                Entity(text="Apple", label="ORG"),
                Entity(text="Apple", label="ORG", start=100, end=105),
            ]
        )
        meta = result.to_metadata()
        assert meta["entities_ORG"] == ["Apple"]


# ═══════════════════════════════════════════════════════════════════
# ZeroShotClassifier._cosine_similarity
# ═══════════════════════════════════════════════════════════════════


class TestCosineSimilarity:
    """Pure math — no ML model needed."""

    def test_identical_vectors(self):
        v = [1.0, 2.0, 3.0]
        assert ZeroShotClassifier._cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert ZeroShotClassifier._cosine_similarity(a, b) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        a = [1.0, 0.0]
        b = [-1.0, 0.0]
        assert ZeroShotClassifier._cosine_similarity(a, b) == pytest.approx(-1.0)

    def test_zero_vector_returns_zero(self):
        assert ZeroShotClassifier._cosine_similarity([0.0, 0.0], [1.0, 2.0]) == 0.0

    def test_both_zero_returns_zero(self):
        assert ZeroShotClassifier._cosine_similarity([0.0], [0.0]) == 0.0


@pytest.mark.asyncio
async def test_legacy_brain_ner_never_accepts_a_model_provider() -> None:
    """Brain enrichment stays local; Router owns every model-provider call."""
    assert "llm_provider" not in signature(EntityExtractor.extract).parameters

    entities = await EntityExtractor(default_mode="gateway").extract("SKU-ABC123")

    assert [(entity.text, entity.label) for entity in entities] == [("SKU-ABC123", "SKU")]
