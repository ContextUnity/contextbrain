"""Public BrainCell contract guard for the Phase 4 legacy-surface cutover."""

from __future__ import annotations

from pathlib import Path

from contextunity.core import brain_pb2
from contextunity.core.sdk import BrainClient


def test_braincell_public_rpc_surface_is_canonical() -> None:
    service = brain_pb2.DESCRIPTOR.services_by_name["BrainService"]
    rpc_names = {method.name for method in service.methods}

    assert {"SearchCells", "IngestDocument", "UpsertCell", "QueryCells", "GetCell"} <= rpc_names
    assert {"GraphSearch", "CreateKGRelation"} <= rpc_names
    assert {"Search", "Upsert", "QueryMemory"}.isdisjoint(rpc_names)


def test_handler_ownership_is_split_without_legacy_facade() -> None:
    handlers = Path(__file__).resolve().parents[2] / "src/contextunity/brain/service/handlers"
    assert not (handlers / "knowledge.py").exists()
    assert (handlers / "cell_search.py").is_file()
    assert (handlers / "cell_write.py").is_file()
    assert (handlers / "cell_edges.py").is_file()


def test_brain_client_exposes_only_canonical_cell_search_and_ingestion() -> None:
    assert hasattr(BrainClient, "search_cells")
    assert hasattr(BrainClient, "ingest_document")
    assert not hasattr(BrainClient, "search")
    assert not hasattr(BrainClient, "upsert")
