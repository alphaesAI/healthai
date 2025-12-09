"""Connector registration module."""

from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector
from .gmail import GmailConnector
from .factory import ConnectorFactory


# Register available connectors
ConnectorFactory.register("postgres", PostgresConnector)
ConnectorFactory.register("postgresql", PostgresConnector)
ConnectorFactory.register("elasticsearch", ElasticsearchConnector)
ConnectorFactory.register("es", ElasticsearchConnector)
ConnectorFactory.register("gmail", GmailConnector)
