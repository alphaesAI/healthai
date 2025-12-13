"""
Simple manual test runner for extractor layer.
Validates:
1. Extractors load via factory
2. Extractors delegate to connectors correctly
3. Allowed endpoints work
"""

import os
from pipeline.connectors.manager import ConnectorManager
from pipeline.extractors.factory import ExtractorFactory

CONNECTOR_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "connectors/connector_config.yml",
)


def test_postgres_extractor():
    print("\n[TEST] Postgres Extractor")

    cm = ConnectorManager(CONNECTOR_CONFIG)
    connector = cm.get_connector("postgres")
    connector.connect()

    extractor = ExtractorFactory.create("postgres", connector)

    assert extractor.test_connection() is True

    # Test extraction with a simple query
    result = list(extractor.extract(query="SELECT 1 AS ok;"))
    print("Query result:", result)

    connector.disconnect()
    print("[PASS] Postgres extractor")


def test_elasticsearch_extractor():
    print("\n[TEST] Elasticsearch Extractor")

    cm = ConnectorManager(CONNECTOR_CONFIG)
    connector = cm.get_connector("elasticsearch")
    connector.connect()

    extractor = ExtractorFactory.create("elasticsearch", connector)

    assert extractor.test_connection() is True

    # Get cluster info through the connector
    info = connector.get_connection_info()
    print("Connection info:", info)

    connector.disconnect()
    print("[PASS] Elasticsearch extractor")


def test_gmail_extractor():
    print("\n[TEST] Gmail Extractor")

    cm = ConnectorManager(CONNECTOR_CONFIG)
    connector = cm.get_connector("gmail")
    connector.connect()

    extractor = ExtractorFactory.create("gmail", connector)

    assert extractor.test_connection() is True

    # Test extraction of unread messages
    messages = list(extractor.extract())
    print(f"Found {len(messages)} unread messages")
    if messages:
        print("First message subject:", messages[0].get('subject', 'No subject'))

    connector.disconnect()
    print("[PASS] Gmail extractor")


if __name__ == "__main__":
    print("Starting extractor tests...")

    test_postgres_extractor()
    test_elasticsearch_extractor()
    test_gmail_extractor()

    print("\nAll extractor tests completed successfully.")
