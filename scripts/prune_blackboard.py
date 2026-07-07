#!/usr/bin/env python3
"""Prune expired Blackboard records from an operator shell.

Local/in-process Brain starts its own Blackboard janitor. This script is the
operator counterpart for service deployments, manual maintenance, or an
external scheduler. Safe to run repeatedly; deletion is idempotent.

Usage:
    # All tenants:
    BRAIN_DATABASE_URL=postgresql://... uv run python -m scripts.prune_blackboard

    # Single tenant:
    BRAIN_DATABASE_URL=postgresql://... uv run python -m scripts.prune_blackboard --tenant contextmed

    # Optional external schedule (every 5 minutes):
    */5 * * * * cd /path/to/services/brain && BRAIN_DATABASE_URL=... uv run python -m scripts.prune_blackboard
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys


async def prune_blackboard(dsn: str, *, tenant_id: str | None) -> int:
    """Delete expired Blackboard records; return the number deleted."""
    from contextunity.brain.storage.postgres import PostgresBrainStore

    store = PostgresBrainStore(dsn=dsn)
    try:
        deleted = await store.prune_expired_blackboard(tenant_id=tenant_id)
        print(f"Blackboard prune: deleted={deleted} tenant={tenant_id or 'all'}")
        return deleted
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune expired Blackboard records")
    parser.add_argument(
        "--tenant",
        default=None,
        help="Prune only this tenant (default: all tenants)",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Database URL (default: BRAIN_DATABASE_URL or DATABASE_URL env)",
    )
    args = parser.parse_args()

    dsn = args.dsn or os.getenv("BRAIN_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("Error: Set BRAIN_DATABASE_URL or pass --dsn", file=sys.stderr)
        sys.exit(1)

    asyncio.run(prune_blackboard(dsn, tenant_id=args.tenant))


if __name__ == "__main__":
    main()
