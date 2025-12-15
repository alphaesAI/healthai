# pipeline/extractors/postgres.py
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .registry import ExtractorRegistry


class PostgresExtractor(BaseExtractor):
    def __init__(self, name: str, connector, config: Dict[str, Any]):
        super().__init__(name, connector, config)

    def extract(self) -> Iterator[Dict[str, Any]]:
        """Extract data from PostgreSQL table."""
        table_name = self.config.get('table_name')
        schema = self.config.get('schema', 'public')
        columns = self.config.get('columns')
        extraction_mode = self.config.get('extraction_mode', 'full')
        date_column = self.config.get('date_column')
        batch_size = self.config.get('batch_size', 1000)
        order_by = self.config.get('order_by')
        
        # Build query
        if columns:
            columns_str = ', '.join(columns)
        else:
            columns_str = '*'
        
        query = f"SELECT {columns_str} FROM {schema}.{table_name}"
        
        # Add incremental filter if specified
        if extraction_mode == 'incremental_date' and date_column:
            # For now, just extract all data - state management would be added here
            pass
        
        # Add ordering
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # Execute query
        rows = self.connector.execute_query(query)
        
        for row in rows:
            yield row


# Register the extractor
ExtractorRegistry.register("postgres", PostgresExtractor)
