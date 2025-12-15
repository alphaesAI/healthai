#!/usr/bin/env python3
"""
Simple test of the connector and extractor pipeline.
"""

from pipeline.connectors.manager import ConnectorManager
from pipeline.extractors.manager import ExtractorManager

def main():
    print("Starting pipeline test...")
    
    # 1. Initialize managers
    connector_manager = ConnectorManager()
    extractor_manager = ExtractorManager()
    
    # 2. List available connectors and extractors
    print("\nAvailable connectors:", connector_manager.list_connectors())
    print("Available extractors:", extractor_manager.list_extractors())
    
    # 3. Test a specific extractor
    try:
        extractor_name = extractor_manager.list_extractors()[0]  # Get first extractor
        print(f"\nRunning extractor: {extractor_name}")
        
        # This will automatically create the required connector
        extractor = extractor_manager.get_extractor(extractor_name)
        
        # Run the extraction
        extractor_manager.run_extraction(extractor_name)
        print(f"Successfully extracted data for {extractor_name}")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
    
    print("\nPipeline test completed")

if __name__ == "__main__":
    main()