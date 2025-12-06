from typing import Dict, List, Optional, Any, Iterator
from ..connectors.elasticsearch import ElasticsearchConnector

class ElasticsearchDB:
    """Elasticsearch database implementation for document storage."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Elasticsearch database.
        
        Args:
            config: Configuration dictionary containing:
                - connection_id: Airflow connection ID
                - index_name: Name of the index (default: 'documents')
                - mappings: Custom index mappings (optional)
        """
        self.config = config
        self.index_name = config.get('index_name', 'documents')
        self.connector = ElasticsearchConnector(config)
        self.connector.connect()  # Connect to Elasticsearch
        self._ensure_index()
    
    def _ensure_index(self) -> None:
        """Ensure the index exists with proper mappings."""
        try:
            if not hasattr(self.connector, 'index_exists') or not self.connector.index_exists(self.index_name):
                # Get custom mappings or use defaults
                custom_mappings = self.config.get('mappings', {})
                if custom_mappings and 'properties' in custom_mappings:
                    # Use provided mappings
                    body = {"mappings": custom_mappings}
                else:
                    # Use default mappings
                    body = {
                        "mappings": {
                            "properties": {
                                "id": {"type": "keyword"},
                                "text": {"type": "text"},
                                "tags": {"type": "nested"},
                                "metadata": {"type": "object", "enabled": False}
                            }
                        }
                    }
                self.connector.create_index(self.index_name, body=body)
        except Exception as e:
            print(f"Error ensuring index exists: {e}")
    
    def insert(self, documents: List[Dict[str, Any]], offset: int = None) -> bool:
        """Insert documents into the database.
        
        Args:
            documents: List of documents to insert (can be tuples or dicts)
            offset: Optional offset (ignored for Elasticsearch)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            actions = []
            for doc in documents:
                # Handle both tuple format (id, text, tags) and dict format
                if isinstance(doc, tuple) and len(doc) >= 2:
                    doc_id = doc[0]
                    text = doc[1]
                    tags = doc[2] if len(doc) > 2 else None
                    # Convert to document format
                    doc_dict = {
                        'id': doc_id,
                        'text': text,
                        'tags': tags
                    }
                elif isinstance(doc, dict):
                    doc_dict = doc
                    doc_id = doc_dict.get('id')
                else:
                    continue
                
                actions.append({
                    "_op_type": "index", 
                    "_index": self.index_name, 
                    "_id": doc_id, 
                    "_source": doc_dict
                })
            result = self.connector.bulk(actions)
            # Handle both boolean and dictionary responses
            if isinstance(result, bool):
                return result
            elif isinstance(result, dict):
                # Check if success_count > 0
                return result.get('success_count', 0) > 0
            return False
        except Exception as e:
            print(f"Error inserting documents: {e}")
            return False
    
    def get(self, ids: List[str]) -> List[Dict[str, Any]]:
        """Get documents by their IDs.
        
        Args:
            ids: List of document IDs to retrieve
            
        Returns:
            List of matching documents
        """
        query = {"query": {"ids": {"values": ids}}}
        try:
            response = self.connector.search(index=self.index_name, body=query)
            return [hit['_source'] for hit in response['hits']['hits']]
        except Exception as e:
            print(f"Error getting documents: {e}")
            return []
    
    def delete(self, ids: List[str]) -> bool:
        """Delete documents by their IDs.
        
        Args:
            ids: List of document IDs to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            query = {"query": {"ids": {"values": ids}}}
            self.connector.delete_by_query(index=self.index_name, body=query)
            return True
        except Exception as e:
            print(f"Error deleting documents: {e}")
            return False
    
    def count(self) -> int:
        """Get the number of documents in the index.
        
        Returns:
            int: Number of documents
        """
        try:
            return self.connector.count(index=self.index_name)
        except Exception as e:
            print(f"Error counting documents: {e}")
            return 0
    
    def close(self) -> None:
        """Close the database connection."""
        self.connector.close()
