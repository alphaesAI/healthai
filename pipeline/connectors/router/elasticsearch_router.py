"""Router for Elasticsearch connector endpoints."""

from typing import Any, Dict
from .base_router import BaseRouter


class ElasticsearchRouter(BaseRouter):
    """Router for Elasticsearch connector endpoints."""
    
    def route_request(self, endpoint: str, params: Dict[str, Any]) -> Any:
        """Route request to appropriate Elasticsearch method."""
        if not self.validate_endpoint(endpoint):
            raise ValueError(f"Unknown endpoint: {endpoint}")
        
        if endpoint == "search":
            index = params.get("index")
            query = params.get("query", {})
            size = params.get("size", 10)
            return self.connector.search(index, query, size)
        
        elif endpoint == "index_document":
            index = params.get("index")
            doc_id = params.get("doc_id")
            body = params.get("body")
            return self.connector.index_document(index, doc_id, body)
        
        elif endpoint == "get_document":
            index = params.get("index")
            doc_id = params.get("doc_id")
            return self.connector.get_document(index, doc_id)
        
        elif endpoint == "delete_document":
            index = params.get("index")
            doc_id = params.get("doc_id")
            return self.connector.delete_document(index, doc_id)
        
        elif endpoint == "bulk_index":
            actions = params.get("actions")
            return self.connector.bulk_index(actions)
        
        elif endpoint == "count":
            index = params.get("index")
            body = params.get("body")
            return self.connector.count(index, body)
        
        elif endpoint == "test_connection":
            return self.connector.test_connection()
        
        elif endpoint == "get_connection_info":
            return self.connector.get_connection_info()
        
        else:
            raise ValueError(f"Endpoint '{endpoint}' not implemented")
    
    def get_available_endpoints(self) -> list[str]:
        """Get list of available Elasticsearch endpoints."""
        return [
            "search",
            "index_document",
            "get_document", 
            "delete_document",
            "bulk_index",
            "count",
            "test_connection",
            "get_connection_info"
        ]
