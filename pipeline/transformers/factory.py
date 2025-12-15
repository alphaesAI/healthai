# pipeline/transformers/factory.py
from typing import Any
from .registry import TransformerRegistry
from .base import BaseTransformer


class TransformerFactory:
    """
    Factory for creating transformers.
    """

    @staticmethod
    def create(transformer_type: str, name: str = None, **kwargs) -> BaseTransformer:
        transformer_cls = TransformerRegistry.get(transformer_type)
        return transformer_cls(name=name or transformer_type, **kwargs)
