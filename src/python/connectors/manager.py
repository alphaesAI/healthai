"""Manager class for handling multiple connectors."""

from typing import Any, Dict
from .base import BaseConnector
from .factory import ConnectorFactory
from .config import config_manager


class ConnectorManager:
    """Manager class for handling multiple connectors."""
    
    def __init__(self):
        self._connectors: Dict[str, BaseConnector] = {}
    
    def get_connector(self, connector_name: str) -> BaseConnector:
        """Get or create a connector by name."""
        if connector_name not in self._connectors:
            self._connectors[connector_name] = self._create_connector(connector_name)
        return self._connectors[connector_name]
    
    def _create_connector(self, connector_name: str) -> BaseConnector:
        """Create a connector from configuration."""
        connector_config = config_manager.get_connector_config(connector_name)
        connector_type = connector_config.get('type')
        
        if not connector_type:
            raise ValueError(f"Connector '{connector_name}' missing 'type' field")
        
        return ConnectorFactory.create(connector_type, connector_config)
    
    def list_connectors(self) -> list[str]:
        """List all configured connector names."""
        return config_manager.list_connectors()
    
    def test_all_connections(self) -> Dict[str, bool]:
        """Test all configured connections."""
        results = {}
        for name in self.list_connectors():
            try:
                connector = self.get_connector(name)
                results[name] = connector.test_connection()
            except Exception:
                results[name] = False
        
        return results
    
    def disconnect_all(self) -> None:
        """Disconnect all connectors."""
        for connector in self._connectors.values():
            try:
                connector.disconnect()
            except Exception:
                pass
        
        self._connectors.clear()
