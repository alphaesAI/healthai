"""Configuration management for extractors."""

import os
import yaml
from typing import Any, Dict


class ExtractorConfigManager:
    """Manages configuration loading and validation for extractors."""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Check environment variable first
            config_path = os.getenv('EXTRACTORS_CONFIG_PATH')
            
            if config_path is None:
                # Get extractor directory and use local config
                extractor_dir = os.path.dirname(__file__)
                config_path = os.path.join(extractor_dir, 'extractor_config.yml')
        
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
    
    def get_extractor_config(self, extractor_name: str) -> Dict[str, Any]:
        """Get configuration for a specific extractor."""
        config = self.load_config()
        extractors = config.get('extractors', {})
        
        if extractor_name not in extractors:
            raise ValueError(f"Extractor '{extractor_name}' not found in config")
        
        return extractors[extractor_name]
    
    def get_default_config(self, extractor_type: str) -> Dict[str, Any]:
        """Get default configuration for an extractor type."""
        config = self.load_config()
        defaults = config.get('defaults', {})
        return defaults.get(extractor_type, {})
    
    def list_extractors(self) -> list[str]:
        """List all configured extractor names."""
        config = self.load_config()
        return list(config.get('extractors', {}).keys())


# Global config manager instance
extractor_config_manager = ExtractorConfigManager()
