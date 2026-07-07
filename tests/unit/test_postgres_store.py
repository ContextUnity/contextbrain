from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from contextunity.brain.storage.postgres import PostgresBrainStore, ScopePath
from contextunity.brain.storage.postgres.store.helpers import _json_safe_row


def _store() -> PostgresBrainStore:
    return PostgresBrainStore(dsn="postgresql://user:pass@localhost/db")


def test_build_scope_filters_basic():
    store = _store()
    where, params = store._build_scope_filters(
        tenant_id="tenant",
        user_id="user",
        scope=ScopePath(path="book.chapter_01"),
        source_types=["book", "video"],
    )
    where_strs = [str(w) for w in where]
    assert any("tenant_id = %s" in s for s in where_strs)
    assert any("scope_path <@ %s::ltree" in s for s in where_strs)
    assert any("source_type = ANY(%s::text[])" in s for s in where_strs)
    assert params == ["tenant", "user", "book.chapter_01", ["book", "video"]]


def test_fuse_results_weighted_handles_missing_scores():
    store = _store()
    ranked = store._fuse_results({"a": 0.9}, {"b": 0.8}, "weighted", 60, 0.8, 0.2, 2)
    ids = [rid for rid, _ in ranked]
    assert set(ids) == {"a", "b"}


def test_fuse_results_rrf_is_deterministic():
    store = _store()
    ranked = store._fuse_results({"a": 0.9, "b": 0.5}, {"b": 0.9, "a": 0.2}, "rrf", 10, 0.8, 0.2, 2)
    ids = [rid for rid, _ in ranked]
    assert set(ids) == {"a", "b"}


def test_postgres_row_normalizer_accepts_scalar_wire_types():
    row = _json_safe_row(
        {
            "total": Decimal("2"),
            "ratio": Decimal("1.5"),
            "created_at": datetime(2026, 7, 4, 12, tzinfo=UTC),
            "id": UUID("12345678-1234-5678-1234-567812345678"),
        }
    )

    assert row == {
        "total": 2,
        "ratio": 1.5,
        "created_at": "2026-07-04T12:00:00+00:00",
        "id": "12345678-1234-5678-1234-567812345678",
    }
