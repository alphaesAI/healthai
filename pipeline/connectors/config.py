"""Configuration management for connectors."""

import os
import yaml
from typing import Any, Dict


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Check environment variable first
            config_path = os.getenv('CONNECTORS_CONFIG_PATH')
            
            if config_path is None:
                # Get connector directory and use local config
                connector_dir = os.path.dirname(__file__)
                config_path = os.path.join(connector_dir, 'connector_config.yml')
        
        self.config_path = config_path
        self._config = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if self._config is None:
            try:
                with open(self.config_path, 'r') as file:
                    self._config = yaml.safe_load(file) or {}
            except FileNotFoundError:
                self._config = {}
            except Exception as e:
                raise ValueError(f"Failed to load config file {self.config_path}: {e}")
        
        return self._config
    
    def get_connector_config(self, connector_name: str) -> Dict[str, Any]:
        """Get configuration for a specific connector."""
        config = self.load_config()
        connectors = config.get('connectors', {})
        
        if connector_name not in connectors:
            raise ValueError(f"Connector '{connector_name}' not found in config")
        
        return connectors[connector_name]
    
    def list_connectors(self) -> list[str]:
        """List all configured connector names."""
        config = self.load_config()
        return list(config.get('connectors', {}).keys())


# Global config manager instance
config_manager = ConfigManager()
