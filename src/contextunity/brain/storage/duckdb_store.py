"""Module providing Module docstring is missing capabilities."""

import duckdb
from contextunity.core import get_contextunit_logger
from contextunity.core.parsing import json_loads as parse_wire_json
from contextunity.core.types import JsonDict, is_json_dict, is_object_list

from contextunity.brain.core.exceptions import BrainValidationError

logger = get_contextunit_logger(__name__)


class DuckDBStore:
    """
    Analytical storage using DuckDB.
    Designed for complex SQL analytics over large datasets.
    """

    def __init__(self, db_path: str = ":memory:"):
        """Initialize a new instance of DuckDBStore.

        Args:
            db_path (str): The db path parameter.
        """
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path)
        logger.info(f"DuckDB storage initialized at {db_path}")

    def import_parquet(self, table_name: str, file_path: str):
        """Import Parquet files for analysis.

        Args:
            table_name (str): The table name parameter.
            file_path (str): The file path parameter.

        Raises:
            ValueError: If parameter values are invalid.
        """
        import re

        if not re.match(r"^[a-zA-Z0-9_]+$", table_name):
            raise BrainValidationError(f"Invalid table name: {table_name}")
        _ = self.conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet(?)",  # noqa: S608  # nosec B608 — table_name validated by regex above
            [file_path],
        )
        logger.info(f"Imported Parquet data to table {table_name}")

    def query(self, sql: str) -> list[JsonDict]:
        """Execute analytical query.

        Args:
            sql (str): The sql parameter.

        Returns:
            List[Dict]: A list of List[Dict].
        """
        try:
            rel = self.conn.execute(sql)
            fetchdf_fn: object = getattr(rel, "fetchdf", None)
            if not callable(fetchdf_fn):
                return []
            df: object = fetchdf_fn()
            to_json_fn: object = getattr(df, "to_json", None)
            if not callable(to_json_fn):
                return []
            records_text = str(to_json_fn(orient="records"))
            parsed = parse_wire_json(records_text)
            if not is_object_list(parsed):
                return []
            out: list[JsonDict] = []
            for item in parsed:
                if is_json_dict(item):
                    out.append(item)
            return out
        except Exception as e:
            logger.error(f"DuckDB Query failed: {e}")
            return []

    def close(self) -> None:
        """Close."""
        closer: object = getattr(self.conn, "close", None)
        if callable(closer):
            _ = closer()
