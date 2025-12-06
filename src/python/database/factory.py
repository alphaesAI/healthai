"""
Factory module
"""

from urllib.parse import urlparse

from ..util import Resolver

from .client import Client
from .duckdb import DuckDB
from .elasticsearch_db import ElasticsearchDB
from .sqlite import SQLite


class DatabaseFactory:
    """
    Methods to create document databases.
    """

    @staticmethod
    def create(config):
        """
        Create a Database.

        Args:
            config: database configuration parameters

        Returns:
            Database
        """

        # Database instance
        database = None

        # Check for explicit database type first
        db_type = config.get("type")
        
        # Enables document database
        content = config.get("content")

        # Standardize content name if no explicit type
        if not db_type and content is True:
            db_type = "sqlite"
        elif not db_type:
            db_type = content

        # Create document database instance
        if db_type == "duckdb":
            database = DuckDB(config)
        elif db_type == "elasticsearch":
            database = ElasticsearchDB(config)
        elif db_type == "sqlite":
            database = SQLite(config)
        elif db_type:
            # Check if db_type is a URL
            url = urlparse(db_type)
            if db_type == "client" or url.scheme:
                # Connect to database server URL
                database = Client(config)
            else:
                # Resolve custom database if db_type is not a URL
                database = DatabaseFactory.resolve(db_type, config)

        # Store config back
        config["type"] = db_type

        return database

    @staticmethod
    def resolve(backend, config):
        """
        Attempt to resolve a custom backend.

        Args:
            backend: backend class
            config: index configuration parameters

        Returns:
            Database
        """

        try:
            return Resolver()(backend)(config)
        except Exception as e:
            raise ImportError(f"Unable to resolve database backend: '{backend}'") from e