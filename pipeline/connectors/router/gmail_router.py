"""Router for Gmail connector endpoints."""

from typing import Any, Dict
from .base_router import BaseRouter


class GmailRouter(BaseRouter):
    """Router for Gmail connector endpoints."""
    
    def route_request(self, endpoint: str, params: Dict[str, Any]) -> Any:
        """Route request to appropriate Gmail method."""
        if not self.validate_endpoint(endpoint):
            raise ValueError(f"Unknown endpoint: {endpoint}")
        
        if endpoint == "test_connection":
            return self.connector.test_connection()
        
        elif endpoint == "get_connection_info":
            return self.connector.get_connection_info()
        
        elif endpoint == "connect":
            return self.connector.connect()
        
        elif endpoint == "disconnect":
            return self.connector.disconnect()
        
        else:
            raise ValueError(f"Endpoint '{endpoint}' not implemented")
    
    def get_available_endpoints(self) -> list[str]:
        """Get list of available Gmail endpoints."""
        return [
            "test_connection",
            "get_connection_info",
            "connect",
            "disconnect"
        ]
