"""
Config-driven PostgreSQL extractor for Sabi ETL pipeline.
"""

import logging
from typing import Any, Dict, Iterator, Optional

import pandas as pd
from sqlalchemy import inspect, text, MetaData

from .base import BaseExtractor

class PostgresExtractor(BaseExtractor):
    def __init__(self, connector, config: dict):
        """
        Args:
            connector: PostgresConnector instance
            config: dictionary of extractor parameters (optional)
        """
        super().__init__(connector, config)
        self.metadata = MetaData()
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get("batch_size", 1000)


    # ────────────────────────────────────────────────
    # Internal helper: convert Timestamp → ISO string
    # ────────────────────────────────────────────────
    def _serialize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        return df

    # ────────────────────────────────────────────────
    # Main extraction
    # ────────────────────────────────────────────────
    def extract(
        self,
        source: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract rows from PostgreSQL using:
        - SQL query (if provided in config or argument)
        - OR table name
        """

        if not self.validate_connection():
            raise RuntimeError("Postgres connection is not valid")

        # Source resolution
        table_name = source or self.config.get("table")

        if not table_name and not query and "query" not in self.config:
            raise ValueError("No table or query provided for PostgresExtractor")

        # Query resolution (priority: function arg → YAML → simple table select)
        sql_query = (
            query
            or self.config.get("query")
            or f"SELECT * FROM {table_name}"
        )

        if "where" in self.config:
            sql_query = f"{sql_query} WHERE {self.config['where']}"

        if kwargs.get("where"):
            sql_query = f"{sql_query} WHERE {kwargs['where']}"

        batch_size = kwargs.get("batch_size", self.batch_size)

        try:
            session = self.connector.get_session()

            offset = 0
            base_sql = text(sql_query)

            while True:
                paginated_sql = base_sql.limit(batch_size).offset(offset)

                result = session.execute(paginated_sql)
                rows = result.fetchall()

                if not rows:
                    break

                columns = result.keys()
                for row in rows:
                    yield dict(zip(columns, row))

                # Pagination
                offset += batch_size
                if len(rows) < batch_size:
                    break

        except Exception as e:
            self.logger.error(f"Postgres extraction failed: {e}")
            raise
        
        finally:
            if "session" in locals():
                session.close()

    # ────────────────────────────────────────────────
    # Full table extraction
    # ────────────────────────────────────────────────
    def extract_full(self, **kwargs) -> Iterator[Dict[str, Any]]:
        return self.extract(**kwargs)

    # ────────────────────────────────────────────────
    # Incremental extraction
    # ────────────────────────────────────────────────
    def extract_incremental(
        self,
        timestamp_column: str,
        last_extracted: Optional[str] = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        table_name = kwargs.get("source") or self.config.get("table")
        if not table_name:
            raise ValueError("Table name is required for incremental extraction")

        where_clause = None
        if last_extracted:
            where_clause = f"{timestamp_column} > '{last_extracted}'"

        return self.extract(
            source=table_name,
            where=where_clause,
            **kwargs
        )

    # ────────────────────────────────────────────────
    # Schema
    # ────────────────────────────────────────────────
    def get_schema(self, source: Optional[str] = None) -> Dict[str, Any]:
        if not self.validate_connection():
            raise RuntimeError("Postgres connection is invalid")

        table_name = source or self.config.get("table")
        if not table_name:
            raise ValueError("table is required to get schema")

        try:
            inspector = inspect(self.connector.engine)
            columns = inspector.get_columns(table_name)

            return {
                "table": table_name,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": col.get("default")
                    }
                    for col in columns
                ]
            }

        except Exception as e:
            self.logger.error(f"Schema fetch failed for {table_name}: {e}")
            raise

    # ────────────────────────────────────────────────
    # List all tables
    # ────────────────────────────────────────────────
    def get_table_list(self) -> list:
        if not self.validate_connection():
            raise RuntimeError("Postgres connection is invalid")

        try:
            inspector = inspect(self.connector.engine)
            return inspector.get_table_names()
        except Exception as e:
            self.logger.error(f"Failed to list tables: {e}")
            raise
