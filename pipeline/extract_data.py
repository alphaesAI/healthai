#!/usr/bin/env python3
"""
Extract actual data using existing configurations.
"""

from pipeline.connectors.manager import ConnectorManager
from pipeline.extractors.manager import ExtractorManager

def main():
    print("=== Data Extraction Demo ===\n")
    
    # Initialize managers
    connector_manager = ConnectorManager()
    extractor_manager = ExtractorManager()
    
    # 1. PostgreSQL FULL Extraction
    print("1. PostgreSQL FULL Extraction:")
    extractor_manager.run_extraction('users')
    print("   Completed")
    
    print()
    
    # 2. PostgreSQL Incremental Date Extraction
    print("2. PostgreSQL INCREMENTAL DATE Extraction:")
    extractor_manager.run_extraction('orders')
    print("   Completed")
    
    print()
    
    # 3. Gmail UNREAD Emails Extraction
    print("3. Gmail UNREAD Emails Extraction:")
    gmail_connector = connector_manager.get_connector('gmail')
    gmail_connector.connect()
    
    gmail_config = {
        'query': 'is:unread',
        'mark_as_read': False
    }
    
    from pipeline.extractors.factory import ExtractorFactory
    extractor = ExtractorFactory.create('gmail', gmail_connector, gmail_config)
    
    data = list(extractor.extract())
    print(f"   Extracted {len(data)} unread emails")
    
    print("\n=== Extraction Complete ===")

if __name__ == "__main__":
    main()
