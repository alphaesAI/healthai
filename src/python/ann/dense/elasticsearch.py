from typing import List, Optional, Tuple, Any, Dict
import numpy as np
from ...connectors.elasticsearch import ElasticsearchConnector

class ElasticsearchDense:
    """Elasticsearch implementation of dense vector index for ANN search."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Elasticsearch dense vector index.
        
        Args:
            config: Configuration dictionary containing:
                - connection_id: Airflow connection ID for Elasticsearch
                - index_name: Name of the index (default: 'dense_vectors')
                - dimension: Dimension of the vectors (default: 384)
                - vector_field: Name of the vector field (default: 'vector')
                - id_field: Name of the ID field (default: 'doc_id')
                - similarity: Similarity metric (default: 'cosine')
        """
        self.config = config
        self.dimension = config.get('dimension', 384)
        self.index_name = config.get('index_name', 'dense_vectors')
        self.vector_field = config.get('vector_field', 'vector')
        self.id_field = config.get('id_field', 'doc_id')
        self.similarity = config.get('similarity', 'cosine')
        
        self.connector = ElasticsearchConnector(config)
        self.connector.connect()  # Connect to Elasticsearch
        self._ensure_index()
    
    def _ensure_index(self) -> None:
        """Ensure the vector index exists with proper mappings."""
        if not self.connector.index_exists(self.index_name):
            mappings = {
                "mappings": {
                    "properties": {
                        self.vector_field: {
                            "type": "dense_vector",
                            "dims": self.dimension,
                            "index": True,
                            "similarity": self.similarity
                        },
                        self.id_field: {"type": "keyword"}
                    }
                }
            }
            # Allow custom mappings to be overridden
            if 'mappings' in self.config:
                mappings['mappings']['properties'].update(self.config['mappings'].get('properties', {}))
            
            self.connector.create_index(self.index_name, body=mappings)
    
    def index(self, vectors: np.ndarray, ids: Optional[List[str]] = None) -> bool:
        """Index vectors with optional document IDs.
        
        Args:
            vectors: Numpy array of shape (n_vectors, dimension)
            ids: Optional list of document IDs corresponding to each vector
            
        Returns:
            bool: True if successful, False otherwise
        """
        if ids is not None and len(ids) != len(vectors):
            raise ValueError("Length of ids must match number of vectors")
            
        try:
            actions = []
            for i, vector in enumerate(vectors):
                doc = {self.vector_field: vector.tolist()}
                if ids is not None:
                    doc[self.id_field] = ids[i]
                actions.append({
                    "_op_type": "index",
                    "_index": self.index_name,
                    "_source": doc
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
            print(f"Error indexing vectors: {e}")
            return False
    
    def search(self, query: np.ndarray, k: int = 10) -> Tuple[List[str], List[float]]:
        """Search for similar vectors.
        
        Args:
            query: Query vector of shape (dimension,)
            k: Number of results to return
            
        Returns:
            Tuple of (doc_ids, scores)
        """
        try:
            script_query = {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, '{self.vector_field}') + 1.0",
                        "params": {"query_vector": query.tolist()}
                    }
                }
            }
            
            response = self.connector.search(
                index=self.index_name,
                body={
                    "query": script_query,
                    "size": k,
                    "_source": [self.id_field]
                }
            )
            
            hits = response['hits']['hits']
            doc_ids = [hit['_source'][self.id_field] for hit in hits]
            scores = [hit['_score'] for hit in hits]
            
            return doc_ids, scores
        except Exception as e:
            print(f"Error searching vectors: {e}")
            return [], []
    
    def delete(self, ids: List[str]) -> bool:
        """Delete vectors by document IDs.
        
        Args:
            ids: List of document IDs to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            query = {"query": {"terms": {self.id_field: ids}}}
            self.connector.delete_by_query(index=self.index_name, body=query)
            return True
        except Exception as e:
            print(f"Error deleting vectors: {e}")
            return False
    
    def count(self) -> int:
        """Get the number of vectors in the index.
        
        Returns:
            int: Number of vectors
        """
        try:
            return self.connector.count(index=self.index_name)
        except Exception as e:
            print(f"Error counting vectors: {e}")
            return 0
    
    def close(self) -> None:
        """Close the Elasticsearch connection."""
        self.connector.close()