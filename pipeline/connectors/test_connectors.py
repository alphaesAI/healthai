#!/usr/bin/env python3
"""Test script for connectors."""

import sys
from pathlib import Path
import yaml
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.connectors.manager import ConnectorManager

def test_connectors():
    """Test all configured connectors."""
    # Use absolute path to config file
    config_path = Path(__file__).parent / 'connector_config.yml'
    manager = ConnectorManager(str(config_path))
    
    print("Available connectors:", manager.list_connectors())
    
    # Test each connector
    for name in manager.list_connectors():
        print(f"\n{'='*50}\nTesting connector: {name}\n{'='*50}")
        try:
            # Get the connector
            connector = manager.get_connector(name)
            
            # Test connection
            print(f"🔌 Connecting to {name}...")
            connector.connect()
            print(f"✅ Connected to {name}")
            
            # Test if connector has execute_query method (for databases)
            if hasattr(connector, 'execute_query'):
                try:
                    if connector.config.get('type') == 'postgres':
                        # Test version
                        result = connector.execute_query("SELECT version() as version")
                        print(f"📊 PostgreSQL Version: {result[0]['version']}")
                        
                        # Get all tables in the database
                        tables = connector.execute_query("""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public'
                        """)
                        
                        if tables:
                            print("\n📋 Available tables:")
                            for table in tables:
                                table_name = table['table_name']
                                print(f"  - {table_name}")
                                
                                # For each table, get row count and sample data
                                try:
                                    count = connector.execute_query(
                                        f"SELECT COUNT(*) as count FROM {table_name}"
                                    )[0]['count']
                                    print(f"    Rows: {count:,}")
                                    
                                    if count > 0:
                                        # Get first 3 rows as sample
                                        sample = connector.execute_query(
                                            f"SELECT * FROM {table_name} LIMIT 3"
                                        )
                                        print("    Sample data:")
                                        for i, row in enumerate(sample, 1):
                                            print(f"    {i}. {row}")
                                            
                                except Exception as e:
                                    print(f"    ⚠️ Could not query table {table_name}: {str(e)}")
                                    
                        else:
                            print("ℹ️ No tables found in the database")
                            
                    elif connector.config.get('type') == 'elasticsearch':
                        # Test ES connection with a simple info query
                        result = connector._client.info()
                        print(f"🌐 ES Cluster: {result.get('cluster_name')}")
                        print(f"🔢 ES Version: {result.get('version', {}).get('number')}")
                        
                        # List all indices
                        indices = connector._client.cat.indices(format='json')
                        if indices:
                            print("\n📋 Available indices:")
                            for idx in indices:
                                print(f"  - {idx['index']} (docs: {idx['docs.count']})")
                except Exception as e:
                    print(f"  ⚠️ Query test failed: {e}")
            
            # Test disconnection
            connector.disconnect()
            print(f"✅ Disconnected from {name}")
            
        except Exception as e:
            print(f"❌ Failed to connect to {name}: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*50)
    print("✅ All connectors tested successfully!")
    print("="*50)

if __name__ == "__main__":
    test_connectors()