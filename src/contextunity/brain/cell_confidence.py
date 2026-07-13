"""Source-type confidence caps for canonical BrainCells (Phase 3 M01)."""

from __future__ import annotations

# High cap: operator-verified manual writes.
# Medium cap: auto-extract, synthesis, and tool/runtime paths.
# Low cap: documentation/test metadata (not user facts).
_CONFIDENCE_CAPS: dict[str, float] = {
    "manual": 1.0,
    "auto_extract": 0.75,
    "synthesis": 0.75,
    "retention": 0.75,
    "memory": 0.75,
    "tool": 0.75,
    "documentation": 0.5,
    "test": 0.5,
}

_DEFAULT_CAP = 0.75


def cap_confidence(source_type: str, confidence: float) -> float:
    """Clamp confidence to the cap for ``source_type`` (anti-theater rule)."""
    cap = _CONFIDENCE_CAPS.get(source_type, _DEFAULT_CAP)
    return max(0.0, min(confidence, cap))


__all__ = ["cap_confidence"]
