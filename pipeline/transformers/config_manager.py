"""Configuration management for transformers."""

import os
import yaml
from typing import Any, Dict


class TransformerConfigManager:
    """Manages configuration loading and validation for transformers."""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Check environment variable first
            config_path = os.getenv('TRANSFORMERS_CONFIG_PATH')
            
            if config_path is None:
                # Get transformer directory and use local config
                transformer_dir = os.path.dirname(__file__)
                config_path = os.path.join(transformer_dir, 'transformer_config.yml')
        
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
    
    def get_transformer_config(self, transformer_name: str) -> Dict[str, Any]:
        """Get configuration for a specific transformer."""
        config = self.load_config()
        transformers = config.get('transformers', {})
        
        if transformer_name not in transformers:
            raise ValueError(f"Transformer '{transformer_name}' not found in config")
        
        return transformers[transformer_name]
    
    def get_default_config(self, transformer_type: str) -> Dict[str, Any]:
        """Get default configuration for a transformer type."""
        config = self.load_config()
        defaults = config.get('defaults', {})
        return defaults.get(transformer_type, {})
    
    def list_transformers(self) -> list[str]:
        """List all configured transformer names."""
        config = self.load_config()
        return list(config.get('transformers', {}).keys())


# Global config manager instance
transformer_config_manager = TransformerConfigManager()
