"""Commerce handlers - products, enrichment, verifications."""

from __future__ import annotations

import json

from contextcore import get_context_unit_logger
from contextcore.exceptions import grpc_error_handler, grpc_stream_error_handler

from ...payloads import GetPendingPayload, SubmitVerificationPayload
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_context_unit_logger(__name__)


class CommerceHandlersMixin:
    """Mixin for Commerce/Gardener operations on BrainService."""

    @grpc_stream_error_handler
    async def GetPendingVerifications(self, request, context):
        """Stream items for manual review (Gardener)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context)
        GetPendingPayload(**unit.payload)

        # TODO: Implement pending items queue
        yield make_response(
            payload={"id": "", "content": "", "context_json": "{}"},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def SubmitVerification(self, request, context):
        """Write enrichment results from Gardener."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context)
        params = SubmitVerificationPayload(**unit.payload)

        try:
            enrichment = json.loads(params.enrichment_json)
        except json.JSONDecodeError:
            enrichment = {}

        logger.info(f"Received verification for {params.id}: {enrichment}")
        return make_response(
            payload={"success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def MatchDuckDB(self, request, context):
        """Execute high-speed DuckDB string/fuzzy matching on Parquet files fetched from S3."""

        import json

        import duckdb

        unit = parse_unit(request)
        token = extract_token_from_context(context)
        # Assuming matching requires write access or a specific scope
        validate_token_for_write(unit, token, context)

        url_unmatched = unit.payload.get("unmatched_url") or unit.payload.get("url_unmatched")
        url_canonical = unit.payload.get("canonical_url") or unit.payload.get("url_canonical")
        url_leftovers_put = unit.payload.get("leftovers_put_url") or unit.payload.get(
            "url_leftovers_put"
        )

        if not url_unmatched or not url_canonical or not url_leftovers_put:
            raise ValueError(
                "unmatched_url, canonical_url, and leftovers_put_url are required in payload"
            )

        logger.info("Running MatchDuckDB on Presigned S3 URLs.")

        try:
            import httpx

            # Local transient in-memory DB to avoid concurrency collisions
            conn = duckdb.connect(":memory:")
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")

            # 1. Exact Matches (SKU/EAN to UPC, or exact Title to Title)
            # Deduplicate: each supplier gets at most ONE match (SKU/EAN preferred over title)
            query_exact = """
                WITH
                  u AS (SELECT * FROM read_parquet(?)),
                  c AS (SELECT * FROM read_parquet(?)),
                  raw_matches AS (
                    SELECT u.supplier_id, u.dealer_code, c.site_id, 'exact_sku' as match_type, 1.0 as confidence, 1 as priority
                    FROM u JOIN c ON u.sku != '' AND u.sku = c.upc
                    UNION ALL
                    SELECT u.supplier_id, u.dealer_code, c.site_id, 'exact_ean' as match_type, 1.0 as confidence, 2 as priority
                    FROM u JOIN c ON u.ean != '' AND u.ean = c.upc
                    UNION ALL
                    SELECT u.supplier_id, u.dealer_code, c.site_id, 'exact_title' as match_type, 0.95 as confidence, 3 as priority
                    FROM u JOIN c ON LOWER(u.name) = LOWER(c.title)
                  ),
                  ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY priority) as rn
                    FROM raw_matches
                  )
                SELECT supplier_id, dealer_code, site_id, match_type, confidence
                FROM ranked WHERE rn = 1
            """
            exact_matches = conn.execute(query_exact, [url_unmatched, url_canonical]).fetchdf()
            matches_dict = exact_matches.to_dict("records")

            # Find the unmatched UUIDs (leftovers)
            query_leftovers = """
                WITH
                  u AS (SELECT * FROM read_parquet(?)),
                  c AS (SELECT * FROM read_parquet(?)),
                  matched_ids AS (
                    SELECT u.supplier_id FROM u JOIN c ON u.sku != '' AND u.sku = c.upc
                    UNION SELECT u.supplier_id FROM u JOIN c ON u.ean != '' AND u.ean = c.upc
                    UNION SELECT u.supplier_id FROM u JOIN c ON LOWER(u.name) = LOWER(c.title)
                  )
                SELECT u.*
                FROM u
                LEFT JOIN matched_ids m ON u.supplier_id = m.supplier_id
                WHERE m.supplier_id IS NULL
            """
            leftovers_df = conn.execute(query_leftovers, [url_unmatched, url_canonical]).fetchdf()
            leftovers_dict = leftovers_df.to_dict("records")

            conn.close()

            # Send the voluminous JSON list back to Cloudflare R2 using the Presigned PUT URL
            async with httpx.AsyncClient() as client:
                resp = await client.put(
                    url_leftovers_put,
                    content=json.dumps(leftovers_dict),
                    headers={"Content-Type": "application/json"},
                    timeout=300.0,
                )
                resp.raise_for_status()

        except Exception as e:
            logger.error(f"DuckDB Matching failed: {e}")
            raise RuntimeError(f"DuckDB Matching failed: {e}")

        logger.info(
            f"MatchDuckDB found {len(matches_dict)} exact matches. Leftovers: {len(leftovers_dict)}."
        )

        return make_response(
            payload={
                "duckdb_matches": matches_dict,
                "duckdb_leftovers_count": len(leftovers_dict),
            },
            parent_unit=unit,
        )


__all__ = ["CommerceHandlersMixin"]
