#!/usr/bin/env python3
"""Initialize Brain database schema from scratch.

Usage:
    # With uv (from contextbrain directory):
    uv run python -m scripts.init_brain

    # With env vars:
    BRAIN_DATABASE_URL=postgresql://brain:brain_dev@localhost:5433/brain \
    PGVECTOR_DIM=1536 \
    uv run python -m scripts.init_brain --commerce --news

    # Or from docker-compose entrypoint:
    python -m scripts.init_brain && python -m contextbrain

This script is idempotent — safe to run multiple times.
All DDL uses IF NOT EXISTS.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys


async def init_brain(
    dsn: str,
    *,
    include_commerce: bool = False,
    include_news: bool = False,
) -> None:
    """Create Brain schema and all tables."""
    from contextbrain.storage.postgres import PostgresKnowledgeStore

    store = PostgresKnowledgeStore(dsn=dsn)

    print("🧠 Initializing Brain schema...")
    print(f"   DSN: {dsn.split('@')[-1] if '@' in dsn else '***'}")
    print(f"   Schema: {store.schema}")
    print(f"   Vector dim: {os.getenv('PGVECTOR_DIM', '1536')}")
    print(f"   Commerce: {include_commerce}")
    print(f"   News: {include_news}")

    await store.ensure_schema(
        include_commerce=include_commerce,
        include_news_engine=include_news,
    )

    print("✅ Brain schema initialized successfully!")
    await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize Brain database schema")
    parser.add_argument(
        "--commerce",
        action="store_true",
        help="Include commerce/taxonomy tables",
    )
    parser.add_argument(
        "--news",
        action="store_true",
        help="Include news engine tables",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Database URL (default: BRAIN_DATABASE_URL or DATABASE_URL env)",
    )
    args = parser.parse_args()

    dsn = args.dsn or os.getenv("BRAIN_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("❌ Error: Set BRAIN_DATABASE_URL or pass --dsn", file=sys.stderr)
        sys.exit(1)

    asyncio.run(
        init_brain(
            dsn,
            include_commerce=args.commerce,
            include_news=args.news,
        )
    )


if __name__ == "__main__":
    main()
