"""ETL Loaders for txtai pipeline system."""

from .base import BaseLoader
from .factory import LoaderFactory
from .elasticsearch import ElasticsearchLoader
from .config_manager import loader_config_manager

# Auto-register loaders
LoaderFactory.register("elasticsearch", ElasticsearchLoader)

__all__ = ["BaseLoader", "LoaderFactory", "ElasticsearchLoader", "loader_config_manager"]
