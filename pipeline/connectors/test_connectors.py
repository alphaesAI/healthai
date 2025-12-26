#!/usr/bin/env python3
"""Test script for connectors."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from .manager import ConnectorManager

def test_connectors():
    """Test all configured connectors."""
    # Use absolute path to config file
    config_path = Path(__file__).parent / 'connector_config.yml'
    manager = ConnectorManager(str(config_path))
    
    print("Available connectors:", manager.list_connectors())
    
    # Test connections
    results = manager.test_all_connections()
    for name, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {name}: {'Connected' if success else 'Failed'}")

if __name__ == "__main__":
    test_connectors()


# from elasticsearch import Elasticsearch

# es = Elasticsearch(["http://localhost:9200"])
# print(es.info())
