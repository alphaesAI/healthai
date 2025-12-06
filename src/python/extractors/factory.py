"""
Factory for creating extractor instances (config-driven).
"""

from typing import Any, Dict, Type
from .base import BaseExtractor


class ExtractorFactory:
    """
    Factory class for creating extractor instances.
    All extractors are registered here and created from YAML config.
    """

    _extractors: Dict[str, Type[BaseExtractor]] = {}

    @classmethod
    def register(cls, extractor_type: str, extractor_class: Type[BaseExtractor]) -> None:
        """
        Register an extractor type.

        Args:
            extractor_type: Identifier for the extractor ("postgres", "gmail", etc.)
            extractor_class: Class implementing BaseExtractor
        """
        cls._extractors[extractor_type] = extractor_class

    @classmethod
    def create(
        cls,
        extractor_type: str,
        connector: Any,
        config: Dict[str, Any]
    ) -> BaseExtractor:
        """
        Create an extractor instance using type + connector + config.

        Args:
            extractor_type: The type of extractor defined in YAML
            connector: The already constructed connector instance
            config: The extractor config section from YAML

        Returns:
            A fully initialized extractor instance
        """
        if extractor_type not in cls._extractors:
            raise ValueError(f"Unknown extractor type: {extractor_type}")

        extractor_class = cls._extractors[extractor_type]

        # Pass both connector + config to the extractor (config-driven)
        return extractor_class(connector=connector, config=config)

    @classmethod
    def list_extractors(cls) -> Dict[str, Type[BaseExtractor]]:
        """
        List all registered extractors.
        Useful for debugging or introspection.
        """
        return cls._extractors.copy()
