import logging
from typing import Dict, List

import duckdb

logger = logging.getLogger(__name__)


class DuckDBStore:
    """
    Analytical storage using DuckDB.
    Designed for complex SQL analytics over large datasets.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        logger.info(f"DuckDB storage initialized at {db_path}")

    def import_parquet(self, table_name: str, file_path: str):
        """Import Parquet files for analysis."""
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
        logger.info(f"Imported Parquet data to table {table_name}")

    def query(self, sql: str) -> List[Dict]:
        """Execute analytical query."""
        try:
            return self.conn.execute(sql).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"DuckDB Query failed: {e}")
            return []

    def close(self):
        self.conn.close()
