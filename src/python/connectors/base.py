from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseConnector(ABC):
    """Base interface for all connectors following SOLID principles."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize connector with configuration."""
        self.config = config
        self._connection = None
        self.connection_id = config.get('connection_id')
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the service."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the service."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is active and working."""
        pass
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information."""
        pass
    
    def is_connected(self) -> bool:
        """Check if connector is currently connected."""
        return self._connection is not None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()