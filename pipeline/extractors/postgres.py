# pipeline/extractors/postgres.py
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .state_manager import StateManager
from .registry import ExtractorRegistry


class PostgresExtractor(BaseExtractor):
    def __init__(self, name: str, connector):
        super().__init__(name, connector)
        self.state = StateManager()

    def extract(self, source: str = None, **kwargs) -> Iterator[Dict[str, Any]]:
        # Default query if none provided
        query = kwargs.get("query", "SELECT * FROM users")
        
        # Add incremental filter if state exists
        last_date = self.state.get(self.name)
        if last_date and "WHERE" not in query:
            query += f" WHERE updated_at > '{last_date}'"
        elif last_date and "WHERE" in query:
            query += f" AND updated_at > '{last_date}'"

        rows = self.connector.execute_query(query=query)

        for row in rows:
            # Row is already a dictionary, yield as-is
            yield row
            # Update state with the last row's timestamp
            if "updated_at" in row:
                self.state.set(self.name, row["updated_at"])

    def test_connection(self) -> bool:
        return self.connector.test_connection()


ExtractorRegistry.register("postgres", PostgresExtractor)
