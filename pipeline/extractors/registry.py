# pipeline/extractors/registry.py
from typing import Dict, Type
from .base import BaseExtractor


class ExtractorRegistry:
    _registry: Dict[str, Type[BaseExtractor]] = {}

    @classmethod
    def register(cls, extractor_type: str, extractor_cls: Type[BaseExtractor]):
        cls._registry[extractor_type] = extractor_cls

    @classmethod
    def get(cls, extractor_type: str) -> Type[BaseExtractor]:
        if extractor_type not in cls._registry:
            raise ValueError(f"Extractor '{extractor_type}' not registered")
        return cls._registry[extractor_type]
