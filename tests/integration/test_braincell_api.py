"""Tests for canonical BrainCell API (M01) — storage + policy helpers.

Uses M00 shared fixtures loader where applicable.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest
import pytest_asyncio
from contextunity.core.exceptions import SecurityError
from contextunity.core.faults import classify_exception
from contextunity.core.types import JsonDict

from contextunity.brain.core.exceptions import BrainCellNotFoundError
from contextunity.brain.service.helpers import resolve_tenant_id, validate_tenant_access
from contextunity.brain.storage.sqlite.store import SqliteBrainStore

_FIXTURE_DIR = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "phase3"


@pytest_asyncio.fixture()
async def store():
    """Fresh sqlite store for cells tests."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test_braincell.db"
        s = SqliteBrainStore(db_path=str(db_path))
        await s.ensure_schema()
        yield s


async def _upsert(store: SqliteBrainStore, **kw) -> JsonDict:
    return await store.upsert_cell(**kw)


async def _query(store: SqliteBrainStore, **kw) -> list[JsonDict]:
    return await store.query_cells(**kw)


async def _get(store: SqliteBrainStore, **kw) -> JsonDict | None:
    return await store.get_cell(**kw)


@pytest.mark.asyncio
async def test_upsert_cell_idempotent_by_content_hash(store: SqliteBrainStore):
    """Same content produces stable id via content_hash; second upsert is no-op change."""
    payload = {
        "tenant_id": "t1",
        "user_id": "u1",
        "cell_kind": "chunk",
        "content": "The quick brown fox.",
        "metadata": {"src": "test"},
    }
    r1 = await _upsert(store, **payload)
    r2 = await _upsert(store, **payload)
    assert r1["id"] == r2["id"]
    assert r1["content_hash"] == r2["content_hash"]


@pytest.mark.asyncio
async def test_query_cells_filters_user_id_kind_and_text(store: SqliteBrainStore):
    """QueryCells honors tenant + user_id + cell_kind + query_text contains + metadata."""
    await _upsert(
        store,
        tenant_id="t1",
        user_id="u1",
        cell_kind="chunk",
        content="alpha about python",
        metadata={"lang": "py"},
    )
    await _upsert(
        store,
        tenant_id="t1",
        user_id="u2",
        cell_kind="concept",
        content="beta about rust",
        metadata={"lang": "rs"},
    )
    await _upsert(
        store, tenant_id="t2", user_id="u1", cell_kind="chunk", content="gamma", metadata={}
    )

    res = await _query(
        store, tenant_id="t1", user_id="u1", cell_kind="chunk", query_text="python", limit=10
    )
    assert len(res) == 1
    assert res[0]["cell_kind"] == "chunk"
    assert "python" in res[0]["content"]
    assert "user_id" not in res[0]

    res2 = await _query(store, tenant_id="t1", metadata_filter={"lang": "rs"}, limit=5)
    assert len(res2) == 1
    assert res2[0]["cell_kind"] == "concept"


@pytest.mark.asyncio
async def test_get_cell_by_id(store: SqliteBrainStore):
    """GetCell returns the record or None."""
    r = await _upsert(store, tenant_id="tX", user_id=None, cell_kind="concept", content="only one")
    got = await _get(store, tenant_id="tX", cell_id=r["id"])
    assert got is not None
    assert got["cell_kind"] == "concept"
    assert "user_id" not in got

    missing = await _get(store, tenant_id="tX", cell_id="nope")
    assert missing is None


@pytest.mark.asyncio
async def test_confidence_cap_applied_on_upsert(store: SqliteBrainStore):
    """auto_extract confidence is capped to the medium tier."""
    result = await _upsert(
        store,
        tenant_id="t1",
        cell_kind="fact",
        content="candidate fact",
        source_type="auto_extract",
        confidence=0.99,
    )
    got = await _get(store, tenant_id="t1", cell_id=result["id"])
    assert got is not None
    meta = got.get("metadata")
    assert isinstance(meta, dict)
    assert meta.get("confidence") == 0.75
    assert got.get("confidence") == 0.75
    assert got.get("visibility") == "tenant"


@pytest.mark.asyncio
async def test_source_ref_persisted(store: SqliteBrainStore):
    result = await _upsert(
        store,
        tenant_id="t1",
        cell_kind="fact",
        content="with ref",
        source_type="manual",
        source_ref="episode:abc",
    )
    got = await _get(store, tenant_id="t1", cell_id=result["id"])
    assert got is not None
    assert got.get("source_ref") == "episode:abc"


def test_tenant_mismatch_classifies_as_policy_fault():
    """M00 tenant_mismatch fixture expectation: policy_fault, not agent_fault."""
    faults = json.loads((_FIXTURE_DIR / "faults_seed_43.json").read_text())
    tenant_mismatch = next(f for f in faults if f["fault"] == "tenant_mismatch")
    token = MagicMock()
    token.allowed_tenants = ("acme_backend",)
    token.can_access_tenant = lambda tid: tid in token.allowed_tenants

    with pytest.raises(SecurityError, match="Tenant access denied") as exc_info:
        validate_tenant_access(
            token, tenant_mismatch["tenant_id"], MagicMock(spec=grpc.ServicerContext)
        )

    assert classify_exception(exc_info.value) == "policy_fault"


def test_payload_tenant_spoofing_classifies_as_policy_fault():
    token = MagicMock()
    token.allowed_tenants = ("project-a",)

    with pytest.raises(SecurityError, match="not in") as exc_info:
        resolve_tenant_id(token, "evil-tenant")

    assert classify_exception(exc_info.value) == "policy_fault"


def test_doc_write_without_authority_classifies_as_policy_fault():
    token = MagicMock()
    token.allowed_tenants = ("acme_backend",)
    token.can_access_tenant = lambda tid: tid in token.allowed_tenants

    with pytest.raises(SecurityError, match="Tenant access denied") as exc_info:
        validate_tenant_access(token, "_doc", MagicMock(spec=grpc.ServicerContext))

    assert classify_exception(exc_info.value) == "policy_fault"


def test_brain_cell_not_found_error_is_contextunity_typed():
    err = BrainCellNotFoundError(tenant_id="t1", cell_id="cell-1")
    assert err.code == "BRAIN_CELL_NOT_FOUND"
    assert err.details["tenant_id"] == "t1"
    assert err.details["cell_id"] == "cell-1"


@pytest.mark.asyncio
async def test_seed_fixture_braincell_roundtrip(store: SqliteBrainStore):
    """M00 braincells_seed_43 loads and upserts deterministically."""
    braincells = json.loads((_FIXTURE_DIR / "braincells_seed_43.json").read_text())
    sample = braincells["acme_backend"][0]
    result = await _upsert(
        store,
        tenant_id=sample["tenant_id"],
        user_id=sample.get("user_id"),
        cell_kind=sample["cell_kind"],
        content=sample["content"],
        metadata=sample.get("metadata", {}),
        source_type=sample.get("source_type", "manual"),
        scope_path=sample.get("scope_path"),
        content_hash=sample.get("content_hash"),
        confidence=sample.get("confidence", 0.5),
    )
    got = await _get(store, tenant_id=sample["tenant_id"], cell_id=result["id"])
    assert got is not None
    assert got["content"] == sample["content"]
