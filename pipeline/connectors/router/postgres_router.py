"""Router for PostgreSQL connector endpoints."""

from typing import Any, Dict
from .base_router import BaseRouter


class PostgresRouter(BaseRouter):
    """Router for PostgreSQL connector endpoints."""
    
    def route_request(self, endpoint: str, params: Dict[str, Any]) -> Any:
        """Route request to appropriate PostgreSQL method."""
        if not self.validate_endpoint(endpoint):
            raise ValueError(f"Unknown endpoint: {endpoint}")
        
        if endpoint == "execute_query":
            query = params.get("query")
            query_params = params.get("params")
            return self.connector.execute_query(query, query_params)
        
        elif endpoint == "execute_many":
            query = params.get("query")
            params_list = params.get("params_list")
            return self.connector.execute_many(query, params_list)
        
        elif endpoint == "test_connection":
            return self.connector.test_connection()
        
        elif endpoint == "get_connection_info":
            return self.connector.get_connection_info()
        
        else:
            raise ValueError(f"Endpoint '{endpoint}' not implemented")
    
    def get_available_endpoints(self) -> list[str]:
        """Get list of available PostgreSQL endpoints."""
        return [
            "execute_query",
            "execute_many", 
            "test_connection",
            "get_connection_info"
        ]
