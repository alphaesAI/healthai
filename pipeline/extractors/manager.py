# pipeline/extractors/manager.py
import os
import yaml
from typing import Any, Dict
from .factory import ExtractorFactory
from ..connectors.manager import ConnectorManager


class ExtractorManager:
    """Manager class for handling multiple extractors."""
    
    def __init__(self, config_path: str = None):
        """Initialize ExtractorManager with optional config path.
        
        Args:
            config_path: Path to YAML config file. If None, uses 'extractor_config.yml'
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'extractor_config.yml')
        
        self.config_path = config_path
        self.connector_manager = ConnectorManager()
        self._extractors: Dict[str, Any] = {}
        self._config: Dict[str, Any] = None
    
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
    
    def get_extractor(self, name: str):
        """Get or create an extractor by name."""
        if name not in self._extractors:
            self._extractors[name] = self._create_extractor(name)
        return self._extractors[name]
    
    def _create_extractor(self, name: str):
        """Create an extractor from configuration."""
        config = self.load_config()
        extractors = config.get('extraction', {}).get('tables', [])
        
        # Find extractor config by table name
        extractor_config = None
        for extractor in extractors:
            if extractor.get('table_name') == name:
                extractor_config = extractor
                break
        
        if not extractor_config:
            raise ValueError(f"Extractor '{name}' not found in config")
        
        # Get connector name from config or use default
        connector_name = extractor_config.get('connector', 'postgres')
        connector = self.connector_manager.get_connector(connector_name)
        
        # Create extractor
        return ExtractorFactory.create('postgres', connector, extractor_config)
    
    def list_extractors(self) -> list[str]:
        """List all configured extractor names."""
        config = self.load_config()
        tables = config.get('extraction', {}).get('tables', [])
        return [table.get('table_name') for table in tables]
    
    def run_extraction(self, name: str) -> None:
        """Run extraction for a specific extractor."""
        extractor = self.get_extractor(name)
        
        # Connect if needed
        if not extractor.connector.is_connected():
            extractor.connector.connect()
        
        # Extract data
        data = list(extractor.extract())
        
        # Write to project storage
        output_path = f"extractors/{name}.json"
        from .writer import write
        write(data, output_path)
    
    def run_all_extractions(self) -> None:
        """Run all configured extractions."""
        for name in self.list_extractors():
            self.run_extraction(name)
