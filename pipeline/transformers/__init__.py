"""ETL Transformers for txtai pipeline system."""

from .base import BaseTransformer
from .factory import TransformerFactory
from .json_transformer import JsonTransformer
from .data_transformer import DataTransformer
from .config_manager import transformer_config_manager

# Auto-register transformers
TransformerFactory.register("json", JsonTransformer)
TransformerFactory.register("data_transformer", DataTransformer)


__all__ = ["BaseTransformer", "TransformerFactory", "JsonTransformer", "DataTransformer", "transformer_config_manager"]
