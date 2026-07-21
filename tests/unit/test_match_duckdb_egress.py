from __future__ import annotations

import inspect
import socket
from pathlib import Path

import duckdb
import pytest

from contextunity.brain.core.exceptions import BrainValidationError
from contextunity.brain.service.handlers import commerce


def test_private_object_destination_is_rejected_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 443))
        ],
    )

    with pytest.raises(BrainValidationError, match="non-public"):
        commerce._require_public_destination(
            "https://metadata.example/object?X-Amz-Signature=signed"
        )


def test_mixed_public_private_destination_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ],
    )
    with pytest.raises(BrainValidationError, match="non-public"):
        commerce._require_public_destination("https://bucket.s3.amazonaws.com/object")


def _write_parquet(path: Path, rows: int) -> None:
    escaped = str(path).replace("'", "''")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"COPY (SELECT range AS supplier_id FROM range({rows})) TO '{escaped}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def test_duckdb_connection_has_resource_limits(tmp_path: Path) -> None:
    conn = commerce._duckdb_connect_memory(temp_directory=tmp_path)
    try:
        assert conn.execute("SELECT current_setting('threads')").fetchone() == (2,)
        assert conn.execute("SELECT current_setting('preserve_insertion_order')").fetchone() == (
            False,
        )
        assert conn.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str(tmp_path),
        )
    finally:
        conn.close()


def test_parquet_row_limit_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parquet = tmp_path / "too-many.parquet"
    _write_parquet(parquet, rows=2)
    monkeypatch.setattr(commerce, "_MAX_INPUT_ROWS", 1)
    conn = commerce._duckdb_connect_memory(temp_directory=tmp_path)
    try:
        with pytest.raises(BrainValidationError, match="row limit"):
            commerce._load_bounded_parquet(conn, parquet, table="unmatched")
    finally:
        conn.close()


def test_duckdb_external_access_is_disabled_after_local_materialization(tmp_path: Path) -> None:
    parquet = tmp_path / "input.parquet"
    _write_parquet(parquet, rows=1)
    conn = commerce._duckdb_connect_memory(temp_directory=tmp_path)
    try:
        commerce._load_bounded_parquet(conn, parquet, table="unmatched")
        conn.execute("SET enable_external_access=false")
        conn.execute("SET lock_configuration=true")
        assert conn.execute("SELECT current_setting('enable_external_access')").fetchone() == (
            False,
        )
        assert conn.execute("SELECT current_setting('lock_configuration')").fetchone() == (True,)
    finally:
        conn.close()


def test_duckdb_handler_never_loads_network_extension() -> None:
    source = inspect.getsource(commerce.CommerceHandlersMixin.MatchDuckDB)
    assert "INSTALL httpfs" not in source
    assert "LOAD httpfs" not in source
