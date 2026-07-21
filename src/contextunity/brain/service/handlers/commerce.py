"""Commerce analytical matching handlers."""

from __future__ import annotations

import ipaddress
import json
import socket
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, final, runtime_checkable
from urllib.parse import urlsplit

import grpc
import httpx
from contextunity.core import contextunit_pb2, get_contextunit_logger
from contextunity.core.exceptions import StorageError
from contextunity.core.grpc_errors import grpc_error_handler
from contextunity.core.narrowing import as_json_dict_list, object_attr
from contextunity.core.types import JsonDict

from ...core.exceptions import BrainValidationError
from ...payloads import MatchDuckDBPayload
from ..handler_base import BrainHandlerBase
from ..helpers import (
    extract_token_from_context,
    make_response,
    parse_unit,
    validate_tenant_access,
    validate_token_for_write,
)

logger = get_contextunit_logger(__name__)
_MAX_PARQUET_BYTES = 256 * 1024 * 1024
_MAX_INPUT_ROWS = 100_000
_MAX_RESULT_ROWS = 100_000
_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
_DUCKDB_MEMORY_LIMIT = "512MB"
_DUCKDB_MAX_TEMP_SIZE = "1GB"


def _require_public_destination(url: str) -> None:
    """Resolve an already-validated object URL and reject private destinations."""
    parsed = urlsplit(url)
    hostname = parsed.hostname or ""
    try:
        addresses = {
            item[4][0]
            for item in socket.getaddrinfo(hostname, parsed.port or 443, type=socket.SOCK_STREAM)
        }
    except socket.gaierror as exc:
        raise BrainValidationError("MatchDuckDB object host could not be resolved") from exc
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise BrainValidationError("MatchDuckDB object host resolved to a non-public address")


async def _download_parquet(client: httpx.AsyncClient, url: str, destination: Path) -> None:
    """Fetch one bounded object without redirects before DuckDB sees it."""
    _require_public_destination(url)
    total = 0
    with destination.open("wb") as output:
        async with client.stream(
            "GET",
            url,
            follow_redirects=False,
            timeout=300.0,
        ) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > _MAX_PARQUET_BYTES:
                    raise BrainValidationError("MatchDuckDB input exceeds the maximum object size")
                output.write(chunk)


def _pandas_records(dataframe: object) -> list[JsonDict]:
    """Narrow DuckDB/pandas ``fetchdf()`` rows at the commerce boundary."""
    to_dict_obj: object = getattr(dataframe, "to_dict", None)
    if not callable(to_dict_obj):
        return []
    records_obj: object = to_dict_obj("records")
    return as_json_dict_list(records_obj)


class _DuckDBCursor(Protocol):
    def fetchdf(self) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...


@runtime_checkable
class _FetchOneCursor(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...


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

    def fetchone(self) -> tuple[object, ...] | None:
        if not isinstance(self._inner, _FetchOneCursor):
            raise TypeError("DuckDB cursor missing typed fetchone()")
        return self._inner.fetchone()


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


def _duckdb_connect_memory(*, temp_directory: Path) -> _DuckDBConnection:
    import duckdb

    connection = _DuckDBConnectionAdapter(duckdb.connect(":memory:"))
    connection.execute(f"SET memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
    connection.execute("SET threads=2")
    connection.execute("SET preserve_insertion_order=false")
    safe_temp_directory = str(temp_directory).replace("'", "''")
    connection.execute(f"SET temp_directory='{safe_temp_directory}'")
    connection.execute(f"SET max_temp_directory_size='{_DUCKDB_MAX_TEMP_SIZE}'")
    return connection


def _load_bounded_parquet(
    conn: _DuckDBConnection,
    path: Path,
    *,
    table: str,
) -> None:
    if table not in {"unmatched", "canonical"}:
        raise ValueError("Unexpected MatchDuckDB table name")
    conn.execute(
        f"CREATE TABLE {table} AS SELECT * FROM read_parquet(?) LIMIT {_MAX_INPUT_ROWS + 1}",
        [str(path)],
    )
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    count = row[0] if row else None
    if not isinstance(count, int) or count > _MAX_INPUT_ROWS:
        raise BrainValidationError(f"MatchDuckDB {table} input exceeds the row limit")


def _bounded_records(cursor: _DuckDBCursor, *, label: str) -> list[JsonDict]:
    records = _pandas_records(cursor.fetchdf())
    if len(records) > _MAX_RESULT_ROWS:
        raise BrainValidationError(f"MatchDuckDB {label} exceeds the result row limit")
    return records


class CommerceHandlersMixin(BrainHandlerBase):
    """Mixin for the legacy-independent DuckDB matching operation."""

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
            with TemporaryDirectory(prefix="brain-duckdb-") as temp_dir:
                unmatched_path = Path(temp_dir) / "unmatched.parquet"
                canonical_path = Path(temp_dir) / "canonical.parquet"
                async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
                    await _download_parquet(client, url_unmatched, unmatched_path)
                    await _download_parquet(client, url_canonical, canonical_path)

                # DuckDB receives local bounded files only; it has no network extension.
                conn = _duckdb_connect_memory(temp_directory=Path(temp_dir))
                try:
                    _load_bounded_parquet(conn, unmatched_path, table="unmatched")
                    _load_bounded_parquet(conn, canonical_path, table="canonical")
                    # Prevent later SQL from opening filesystem, URL, or extension resources.
                    conn.execute("SET enable_external_access=false")
                    conn.execute("SET lock_configuration=true")

                    # Deduplicate canonical lookup keys before joining so duplicate
                    # supplier/canonical keys cannot create a many-to-many explosion.
                    query_exact = f"""
                        WITH
                          u AS (SELECT * FROM unmatched),
                          c AS (SELECT * FROM canonical),
                          c_upc AS (
                            SELECT upc, MIN(site_id) AS site_id FROM c
                            WHERE upc != '' GROUP BY upc
                          ),
                          c_title AS (
                            SELECT LOWER(title) AS title_key, MIN(site_id) AS site_id FROM c
                            GROUP BY LOWER(title)
                          ),
                          possible AS (
                            SELECT u.supplier_id, u.dealer_code,
                                   COALESCE(s.site_id, e.site_id, t.site_id) AS site_id,
                                   CASE WHEN s.site_id IS NOT NULL THEN 'exact_sku'
                                        WHEN e.site_id IS NOT NULL THEN 'exact_ean'
                                        ELSE 'exact_title' END AS match_type,
                                   CASE WHEN s.site_id IS NOT NULL OR e.site_id IS NOT NULL
                                        THEN 1.0 ELSE 0.95 END AS confidence,
                                   CASE WHEN s.site_id IS NOT NULL THEN 1
                                        WHEN e.site_id IS NOT NULL THEN 2 ELSE 3 END AS priority
                            FROM u
                            LEFT JOIN c_upc s ON u.sku != '' AND u.sku = s.upc
                            LEFT JOIN c_upc e ON u.ean != '' AND u.ean = e.upc
                            LEFT JOIN c_title t ON LOWER(u.name) = t.title_key
                            WHERE COALESCE(s.site_id, e.site_id, t.site_id) IS NOT NULL
                          ),
                          ranked AS (
                            SELECT *, ROW_NUMBER() OVER (
                              PARTITION BY supplier_id ORDER BY priority, site_id
                            ) AS rn
                            FROM possible
                          )
                        SELECT supplier_id, dealer_code, site_id, match_type, confidence
                        FROM ranked WHERE rn = 1
                        LIMIT {_MAX_RESULT_ROWS + 1}
                    """
                    exact_result = conn.execute(query_exact)
                    matches_dict = _bounded_records(exact_result, label="matches")

                    query_leftovers = f"""
                        WITH
                          u AS (SELECT * FROM unmatched),
                          c AS (SELECT * FROM canonical),
                          c_upc AS (SELECT DISTINCT upc FROM c WHERE upc != ''),
                          c_title AS (SELECT DISTINCT LOWER(title) AS title_key FROM c),
                          matched_ids AS (
                            SELECT DISTINCT u.supplier_id
                            FROM u
                            LEFT JOIN c_upc s ON u.sku != '' AND u.sku = s.upc
                            LEFT JOIN c_upc e ON u.ean != '' AND u.ean = e.upc
                            LEFT JOIN c_title t ON LOWER(u.name) = t.title_key
                            WHERE s.upc IS NOT NULL OR e.upc IS NOT NULL OR t.title_key IS NOT NULL
                          )
                        SELECT u.*
                        FROM u
                        LEFT JOIN matched_ids m ON u.supplier_id = m.supplier_id
                        WHERE m.supplier_id IS NULL
                        LIMIT {_MAX_RESULT_ROWS + 1}
                    """
                    leftovers_result = conn.execute(query_leftovers)
                    leftovers_dict = _bounded_records(leftovers_result, label="leftovers")
                finally:
                    conn.close()

            matches_json = json.dumps(matches_dict, separators=(",", ":")).encode("utf-8")
            leftovers_json = json.dumps(leftovers_dict, separators=(",", ":")).encode("utf-8")
            if len(matches_json) > _MAX_OUTPUT_BYTES:
                raise BrainValidationError("MatchDuckDB matches response exceeds the byte limit")
            if len(leftovers_json) > _MAX_OUTPUT_BYTES:
                raise BrainValidationError("MatchDuckDB leftovers output exceeds the byte limit")

            # Upload only to a validated destination and never forward across redirects.
            _require_public_destination(url_leftovers_put)
            async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
                resp = await client.put(
                    url_leftovers_put,
                    content=leftovers_json,
                    headers={"Content-Type": "application/json"},
                    timeout=300.0,
                    follow_redirects=False,
                )
                _ = resp.raise_for_status()

        except (StorageError, BrainValidationError):
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
