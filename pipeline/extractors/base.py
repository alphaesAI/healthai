"""
Base extractor class for ETL pipeline (config-driven).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional

from ..connectors.manager import ConnectorManager


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    Extractors are fully config-driven and automatically resolve
    their corresponding connector through ConnectorManager.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize extractor with its name and config.

        Args:
            name: Extractor name as defined in YAML
            config: Extractor-specific configuration
        """
        self.name = name
        self.config = config
        self.connector_name = config.get("connector")

        if not self.connector_name:
            raise ValueError(
                f"Extractor '{name}' missing required field 'connector'"
            )

        # Resolve connector from manager
        self.connector_manager = ConnectorManager()
        self.connector = self.connector_manager.get_connector(self.connector_name)

    # ------------- PUBLIC API -------------

    def validate_connection(self) -> bool:
        """Check connector connectivity before extraction."""
        if hasattr(self.connector, "test_connection"):
            return self.connector.test_connection()
        return True

    # ------------- ABSTRACT METHODS -------------

    @abstractmethod
    def extract(
        self,
        source: str,
        query: Optional[str] = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract records from the given source.

        Args:
            source: table name / label / mailbox / index etc.
            query: optional filtering
            **kwargs: extractor-specific parameters

        Yields:
            dict objects representing extracted data
        """
        pass

    @abstractmethod
    def get_schema(self, source: str) -> Dict[str, Any]:
        """
        Return schema metadata for the source.

        Args:
            source: table name / label / index etc.

        Returns:
            dict containing schema fields
        """
        pass
