"""Initialize Brain database schema.

Usage:
    BRAIN_DATABASE_URL=postgresql://... python scripts/init_db.py

Options:
    --include-commerce     Include commerce/taxonomy tables
    --include-news-engine  Include news pipeline news tables
    --vector-dim DIM       Vector dimension (default: from BRAIN_VECTOR_DIM or 1536)
"""

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv


async def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Initialize Brain database schema")
    parser.add_argument(
        "--include-commerce", action="store_true", help="Include commerce/taxonomy tables"
    )
    parser.add_argument(
        "--include-news-engine", action="store_true", help="Include news pipeline news tables"
    )
    parser.add_argument("--vector-dim", type=int, default=None, help="Vector dimension")
    args = parser.parse_args()

    dsn = os.getenv("BRAIN_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        print("Error: BRAIN_DATABASE_URL or DATABASE_URL not found in environment.")
        sys.exit(1)

    vector_dim = args.vector_dim or int(os.getenv("BRAIN_VECTOR_DIM", "1536"))

    # Import schema builder
    from contextbrain.storage.postgres.schema import build_schema_sql

    statements = build_schema_sql(
        vector_dim=vector_dim,
        include_commerce=args.include_commerce,
        include_news_engine=args.include_news_engine,
    )

    # Connect and execute
    from psycopg_pool import AsyncConnectionPool

    # Parse host info for display
    host_info = dsn.split("@")[-1] if "@" in dsn else dsn
    print(f"Connecting to {host_info}...")
    print(f"Vector dimension: {vector_dim}")
    print(f"Include commerce: {args.include_commerce}")
    print(f"Include news_engine: {args.include_news_engine}")

    async with AsyncConnectionPool(dsn, open=False) as pool:
        await pool.open()
        async with pool.connection() as conn:
            print(f"Executing {len(statements)} statements...")
            for i, stmt in enumerate(statements, 1):
                try:
                    await conn.execute(stmt)
                    # Extract first line for logging
                    first_line = stmt.strip().split("\n")[0][:60]
                    print(f"  [{i}/{len(statements)}] ✓ {first_line}...")
                except Exception as e:
                    print(f"  [{i}/{len(statements)}] ✗ Error: {e}")
                    # Continue with other statements (some may be CREATE IF NOT EXISTS)

            await conn.commit()
            print("\n✅ Database schema initialized successfully!")


if __name__ == "__main__":
    asyncio.run(main())
