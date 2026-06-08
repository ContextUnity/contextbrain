"""Payload contract tests — bounded constraints and default verification.

Framework-level tests (required fields, extra=forbid) removed — those test
Pydantic internals, not our domain logic. We keep only tests that catch
actual regressions: bound violations and default value drift.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from contextunity.brain.payloads import (
    GraphSearchPayload,
    MatchDuckDBPayload,
    SearchPayload,
    UpsertFactPayload,
)

# ═══════════════════════════════════════════════════════════════════
# Bounded field constraints
# ═══════════════════════════════════════════════════════════════════


class TestBoundedConstraints:
    """Fields with ge/le constraints must be enforced."""

    def test_graph_search_max_hops_lower_bound(self):
        with pytest.raises(ValidationError, match="max_hops"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 0}
            )

    def test_graph_search_max_hops_upper_bound(self):
        with pytest.raises(ValidationError, match="max_hops"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 100}
            )

    def test_graph_search_max_results_upper_bound(self):
        with pytest.raises(ValidationError, match="max_results"):
            GraphSearchPayload.model_validate(
                {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_results": 5000}
            )

    def test_graph_search_valid_bounds(self):
        payload = GraphSearchPayload.model_validate(
            {"tenant_id": "t", "entrypoint_ids": ["e1"], "max_hops": 5, "max_results": 500}
        )
        assert payload.max_hops == 5
        assert payload.max_results == 500


# ═══════════════════════════════════════════════════════════════════
# Default value verification
# ═══════════════════════════════════════════════════════════════════


class TestDefaults:
    """Catch default value drift that silently changes query behavior."""

    def test_search_defaults(self):
        p = SearchPayload.model_validate({"tenant_id": "t", "query_text": "hello"})
        assert p.limit == 10
        assert p.min_score == 0.0
        assert p.source_types == []

    def test_upsert_fact_fields(self):
        p = UpsertFactPayload.model_validate(
            {"tenant_id": "t", "user_id": "u", "key": "name", "value": "Alice"}
        )
        assert p.key == "name"
        assert p.value == "Alice"
        assert p.confidence == 1.0


class TestMatchDuckDBPayload:
    """Contract between Commerce matcher client and Brain MatchDuckDB RPC."""

    def test_accepts_current_commerce_payload(self):
        payload = MatchDuckDBPayload.model_validate(
            {
                "tenant_id": "tenant-a",
                "unmatched_url": "https://example.test/unmatched.parquet",
                "canonical_url": "https://example.test/canonical.parquet",
                "leftovers_put_url": "https://example.test/leftovers.json",
            }
        )

        assert payload.tenant_id == "tenant-a"
        assert payload.unmatched_url.endswith("unmatched.parquet")

    def test_accepts_legacy_url_aliases_without_extra_fields(self):
        payload = MatchDuckDBPayload.model_validate(
            {
                "tenant_id": "tenant-a",
                "url_unmatched": "https://example.test/unmatched.parquet",
                "url_canonical": "https://example.test/canonical.parquet",
                "url_leftovers_put": "https://example.test/leftovers.json",
            }
        )

        assert payload.unmatched_url.endswith("unmatched.parquet")
        assert payload.canonical_url.endswith("canonical.parquet")
        assert payload.leftovers_put_url.endswith("leftovers.json")
