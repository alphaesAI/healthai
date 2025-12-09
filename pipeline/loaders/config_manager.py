"""Configuration management for loaders."""

import os
import yaml
from typing import Any, Dict


class LoaderConfigManager:
    """Manages configuration loading and validation for loaders."""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Check environment variable first
            config_path = os.getenv('LOADERS_CONFIG_PATH')
            
            if config_path is None:
                # Get loader directory and use local config
                loader_dir = os.path.dirname(__file__)
                config_path = os.path.join(loader_dir, 'loader_config.yml')
        
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
    
    def get_loader_config(self, loader_name: str) -> Dict[str, Any]:
        """Get configuration for a specific loader."""
        config = self.load_config()
        loaders = config.get('loaders', {})
        
        if loader_name not in loaders:
            raise ValueError(f"Loader '{loader_name}' not found in config")
        
        return loaders[loader_name]
    
    def get_default_config(self, loader_type: str) -> Dict[str, Any]:
        """Get default configuration for a loader type."""
        config = self.load_config()
        defaults = config.get('defaults', {})
        return defaults.get(loader_type, {})
    
    def list_loaders(self) -> list[str]:
        """List all configured loader names."""
        config = self.load_config()
        return list(config.get('loaders', {}).keys())


# Global config manager instance
loader_config_manager = LoaderConfigManager()
