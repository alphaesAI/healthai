# pipeline/extractors/manager.py
import yaml
from typing import Dict
from .factory import ExtractorFactory
from pipeline.connectors.manager import ConnectorManager


class ExtractorManager:
    def __init__(self, extractor_config_path: str, connector_config_path: str):
        self.extractor_config_path = extractor_config_path
        self.connector_manager = ConnectorManager(connector_config_path)
        self._extractors: Dict[str, object] = {}

    def _load_config(self):
        with open(self.extractor_config_path) as f:
            return yaml.safe_load(f) or {}

    def get_extractor(self, name: str):
        if name in self._extractors:
            return self._extractors[name]

        config = self._load_config()
        extractor_cfg = config["extractors"][name]

        connector = self.connector_manager.get_connector(extractor_cfg["type"])

        extractor = ExtractorFactory.create(
            name=name,
            extractor_type=extractor_cfg["type"],
            connector=connector,
            config=extractor_cfg
        )

        self._extractors[name] = extractor
        return extractor
