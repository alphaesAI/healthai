# pipeline/extractors/factory.py

from typing import Dict, Type
from .registry import ExtractorRegistry
from .base import BaseExtractor


class ExtractorFactory:
    @staticmethod
    def create(extractor_type: str, connector):
        extractor_cls = ExtractorRegistry.get(extractor_type)
        return extractor_cls(name=extractor_type, connector=connector)
