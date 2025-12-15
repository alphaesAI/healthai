# pipeline/transformers/registry.py
from typing import Type, Dict
from .base import BaseTransformer


class TransformerRegistry:
    """
    Registry for transformer classes.
    """
    _registry: Dict[str, Type[BaseTransformer]] = {}

    @classmethod
    def register(cls, transformer_type: str, transformer_cls: Type[BaseTransformer]):
        cls._registry[transformer_type] = transformer_cls

    @classmethod
    def get(cls, transformer_type: str) -> Type[BaseTransformer]:
        if transformer_type not in cls._registry:
            raise ValueError(f"Transformer '{transformer_type}' not registered")
        return cls._registry[transformer_type]

    @classmethod
    def list_transformers(cls):
        return list(cls._registry.keys())
