"""Commerce handlers - products, enrichment, verifications."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Protocol, final

import grpc
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.exceptions import StorageError
from contextunity.core.grpc_errors import grpc_error_handler, grpc_stream_error_handler
from contextunity.core.narrowing import as_json_dict, as_json_dict_list, object_attr
from contextunity.core.parsing import json_loads
from contextunity.core.types import JsonDict

from ...core.exceptions import BrainValidationError
from ...payloads import GetPendingPayload, MatchDuckDBPayload, SubmitVerificationPayload
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_read,
    validate_token_for_write,
)

logger = get_contextunit_logger(__name__)


def _pandas_records(dataframe: object) -> list[JsonDict]:
    """Narrow DuckDB/pandas ``fetchdf()`` rows at the commerce boundary."""
    to_dict_obj: object = getattr(dataframe, "to_dict", None)
    if not callable(to_dict_obj):
        return []
    records_obj: object = to_dict_obj("records")
    return as_json_dict_list(records_obj)


class _DuckDBCursor(Protocol):
    def fetchdf(self) -> object: ...


class _DuckDBConnection(Protocol):
    def execute(self, query: str, parameters: list[str] | None = None) -> _DuckDBCursor: ...

    def close(self) -> None: ...


@final
class _DuckDBCursorAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def fetchdf(self) -> object:
        fetchdf_fn_obj: object = object_attr(self._inner, "fetchdf")
        if not callable(fetchdf_fn_obj):
            raise TypeError("DuckDB cursor missing fetchdf()")
        fetchdf_fn: Callable[[], object] = fetchdf_fn_obj
        return fetchdf_fn()


@final
class _DuckDBConnectionAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def execute(self, query: str, parameters: list[str] | None = None) -> _DuckDBCursorAdapter:
        execute_fn_obj: object = object_attr(self._inner, "execute")
        if not callable(execute_fn_obj):
            raise TypeError("DuckDB connection missing execute()")
        execute_fn: Callable[..., object] = execute_fn_obj
        if parameters is None:
            result_obj = execute_fn(query)
        else:
            result_obj = execute_fn(query, parameters)
        return _DuckDBCursorAdapter(result_obj)

    def close(self) -> None:
        close_fn_obj: object = object_attr(self._inner, "close")
        if not callable(close_fn_obj):
            raise TypeError("DuckDB connection missing close()")
        close_fn: Callable[[], object] = close_fn_obj
        _ = close_fn()


def _duckdb_connect_memory() -> _DuckDBConnection:
    import duckdb

    return _DuckDBConnectionAdapter(duckdb.connect(":memory:"))


class CommerceHandlersMixin(BrainHandlerBase):
    """Mixin for Commerce/Gardener operations on BrainService."""

    @grpc_stream_error_handler
    async def GetPendingVerifications(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[contextunit_pb2.ContextUnit]:
        """Stream items for manual review (Gardener)."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_read(unit, token, context)
        _ = GetPendingPayload.model_validate(unit.payload or {})

        # TODO: Implement pending items queue
        yield make_response(
            payload={"id": "", "content": "", "context_json": "{}"},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def SubmitVerification(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Write enrichment results from Gardener."""
        unit = parse_unit(request)
        token = extract_token_from_context(context)
        validate_token_for_write(unit, token, context)
        params = SubmitVerificationPayload.model_validate(unit.payload or {})

        try:
            enrichment_raw: object = json_loads(params.enrichment_json)
        except ValueError:
            enrichment_raw = {}
        enrichment: JsonDict = as_json_dict(enrichment_raw)

        logger.info(f"Received verification for {params.id}: {enrichment}")
        return make_response(
            payload={"success": True},
            parent_unit=unit,  # Inherit trace_id and extend provenance
        )

    @grpc_error_handler
    async def MatchDuckDB(
        self,
        request: contextunit_pb2.ContextUnit,
        context: grpc.ServicerContext,
    ) -> contextunit_pb2.ContextUnit:
        """Execute high-speed DuckDB string/fuzzy matching on Parquet files fetched from S3."""

        unit = parse_unit(request)
        token = extract_token_from_context(context)
        # Assuming matching requires write access or a specific scope
        validate_token_for_write(unit, token, context)

        from pydantic import ValidationError

        try:
            duck_params = MatchDuckDBPayload.model_validate(unit.payload or {})
        except ValidationError as exc:
            raise BrainValidationError(
                "unmatched_url, canonical_url, and leftovers_put_url are required in payload"
            ) from exc
        validate_tenant_access(token, duck_params.tenant_id, context)
        url_unmatched = duck_params.unmatched_url
        url_canonical = duck_params.canonical_url
        url_leftovers_put = duck_params.leftovers_put_url

        logger.info("Running MatchDuckDB on Presigned S3 URLs.")

        try:
            import httpx

            # Local transient in-memory DB to avoid concurrency collisions
            conn = _duckdb_connect_memory()
            try:
                _ = conn.execute("INSTALL httpfs;")
                _ = conn.execute("LOAD httpfs;")

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
                exact_result = conn.execute(query_exact, [url_unmatched, url_canonical])
                matches_dict = _pandas_records(exact_result.fetchdf())

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
                leftovers_result = conn.execute(query_leftovers, [url_unmatched, url_canonical])
                leftovers_dict = _pandas_records(leftovers_result.fetchdf())
            finally:
                conn.close()

            # Send the voluminous JSON list back to Cloudflare R2 using the Presigned PUT URL
            async with httpx.AsyncClient() as client:
                resp = await client.put(
                    url_leftovers_put,
                    content=json.dumps(leftovers_dict),
                    headers={"Content-Type": "application/json"},
                    timeout=300.0,
                )
                _ = resp.raise_for_status()

        except StorageError:
            raise
        except Exception as e:  # graceful-degrade: commerce query failure returns error result
            logger.exception("DuckDB Matching failed")
            raise StorageError(f"DuckDB Matching failed: {type(e).__name__}") from e

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
