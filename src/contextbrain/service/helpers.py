"""Helper functions for gRPC service."""

from __future__ import annotations

import uuid

from contextcore import ContextUnit, context_unit_pb2


def parse_unit(request) -> ContextUnit:
    """Parse protobuf request to ContextUnit."""
    return ContextUnit.from_protobuf(request)


def make_response(
    payload: dict,
    trace_id: str | None = None,
    provenance: list[str] | None = None,
) -> bytes:
    """Create ContextUnit response protobuf."""
    unit = ContextUnit(
        payload=payload,
        trace_id=trace_id or uuid.uuid4(),
        provenance=provenance or ["brain:response"],
    )
    return unit.to_protobuf(context_unit_pb2)


__all__ = ["parse_unit", "make_response"]
