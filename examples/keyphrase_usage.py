"""Example: Using KeyphraseTransformer in ContextRouter (not wired into graphs).

Run:
  uv run python examples/keyphrase_usage.py
"""

from __future__ import annotations

import asyncio

from contextbrain.modules.transformers.keyphrases import KeyphraseTransformer
from contextcore import ContextUnit


async def main() -> None:
    transformer = KeyphraseTransformer()
    transformer.configure({"mode": "llm", "max_phrases": 12, "min_score": 0.15})

    text = """
    ContextRouter is a modular, LangGraph-powered shared brain designed for high-performance
    agentic workflows. It uses the ContextUnit protocol for provenance
    and security. The system separates orchestration (cortex) from modules/providers/connectors.
    """

    envelope = ContextUnit(
        payload={"content": {"content": text}, "metadata": {"source": "example"}}
    )
    enriched = await transformer.transform(envelope)

    keyphrases = (
        enriched.payload.get("metadata", {}).get("keyphrases", []) if enriched.payload else []
    )
    print("=== Keyphrases ===")
    for kp in keyphrases:
        print(f"- {kp['text']} (score={kp['score']:.2f})")


if __name__ == "__main__":
    asyncio.run(main())
