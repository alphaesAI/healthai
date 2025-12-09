"""Base router for connector endpoints."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseRouter(ABC):
    """Base class for connector routers."""
    
    def __init__(self, connector, config: Dict[str, Any]):
        self.connector = connector
        self.config = config
    
    @abstractmethod
    def route_request(self, endpoint: str, params: Dict[str, Any]) -> Any:
        """Route request to appropriate connector method."""
        pass
    
    @abstractmethod
    def get_available_endpoints(self) -> list[str]:
        """Get list of available endpoints."""
        pass
    
    def validate_endpoint(self, endpoint: str) -> bool:
        """Validate if endpoint exists."""
        return endpoint in self.get_available_endpoints()
