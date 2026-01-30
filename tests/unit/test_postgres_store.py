from __future__ import annotations

from contextbrain.storage.postgres import PostgresKnowledgeStore, TaxonomyPath


def _store() -> PostgresKnowledgeStore:
    return PostgresKnowledgeStore(dsn="postgresql://user:pass@localhost/db")


def test_build_scope_filters_basic():
    store = _store()
    where, params = store._build_scope_filters(
        tenant_id="tenant",
        user_id="user",
        scope=TaxonomyPath(path="book.chapter_01"),
        source_types=["book", "video"],
    )
    where_strs = [str(w) for w in where]
    assert any("tenant_id = %s" in s for s in where_strs)
    assert any("taxonomy_path <@ %s::ltree" in s for s in where_strs)
    assert any("source_type = ANY(%s::text[])" in s for s in where_strs)
    assert params == ["tenant", "user", "book.chapter_01", ["book", "video"]]


def test_fuse_results_weighted_handles_missing_scores():
    store = _store()
    ranked = store._fuse_results(
        {"a": 0.9}, {"b": 0.8}, "weighted", 60, 0.8, 0.2, 2
    )
    ids = [rid for rid, _ in ranked]
    assert set(ids) == {"a", "b"}


def test_fuse_results_rrf_is_deterministic():
    store = _store()
    ranked = store._fuse_results(
        {"a": 0.9, "b": 0.5}, {"b": 0.9, "a": 0.2}, "rrf", 10, 0.8, 0.2, 2
    )
    ids = [rid for rid, _ in ranked]
    assert set(ids) == {"a", "b"}
