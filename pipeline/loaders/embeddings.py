"""
Embeddings Module - Vector generation using txtai
"""

from typing import List, Dict, Any, Optional
import numpy as np
from txtai.embeddings import Embeddings


class EmbeddingAligner:
    """
    Vector generation and alignment using txtai.
    
    Responsibilities:
    - Import Embeddings from txtai
    - Use txtai only to generate vectors
    - NEVER let txtai index anything
    - Perform index alignment
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the embedding aligner.
        
        Args:
            config: Embedding configuration following txtai's Embeddings format
        """
        self.config = config
        self.embeddings = None
        self._initialize_embeddings()
    
    def _initialize_embeddings(self):
        """Initialize txtai Embeddings without indexing."""
        # Create embeddings instance without storage/indexing
        embeddings_config = {
            "path": self.config.get("path"),
            "content": self.config.get("content", False),  # Disable content storage by default
            "scoring": self.config.get("scoring", None),  # Disable scoring by default
            "backend": self.config.get("backend", "numpy")  # Use numpy backend by default
        }
        
        # Remove None values from config
        embeddings_config = {k: v for k, v in embeddings_config.items() if v is not None}
        
        # Add any additional txtai Embeddings parameters from config
        for key, value in self.config.items():
            if key not in ["path", "enabled", "content", "scoring", "backend"]:
                embeddings_config[key] = value
        
        self.embeddings = Embeddings(embeddings_config)
    
    def align_and_embed(self, chunks: List['Chunk']) -> List[Dict[str, Any]]:
        """
        Generate vectors for chunks and align metadata.
        
        Args:
            chunks: List of Chunk objects with source_id, chunk_id, text, metadata
            
        Returns:
            List of aligned records with vectors
        """
        if not chunks:
            return []
        
        # Prepare data for txtai: (chunk_id, text, tags)
        txtai_input = []
        chunk_mapping = {}  # Map chunk_id to original chunk
        
        for chunk in chunks:
            txtai_input.append((chunk.chunk_id, chunk.text, chunk.metadata))
            chunk_mapping[chunk.chunk_id] = chunk
        
        # Generate vectors using txtai (embedding only, no indexing)
        # Generate vectors for each text individually
        vectors_list = []
        for item in txtai_input:
            vector = self.embeddings.transform([item[1]])  # Transform each text individually
            vectors_list.append(vector)
        
        # Create aligned records
        aligned_records = []
        for i, (chunk_id, text, tags) in enumerate(txtai_input):
            original_chunk = chunk_mapping[chunk_id]
            
            vector = vectors_list[i].tolist() if hasattr(vectors_list[i], 'tolist') else vectors_list[i]
            
            aligned_record = {
                'source_id': original_chunk.source_id,
                'chunk_id': chunk_id,
                'text': text,
                'metadata': tags,
                'vector': vector
            }
            aligned_records.append(aligned_record)
        
        return aligned_records
    
    def generate_vectors(self, texts: List[str]) -> List[List[float]]:
        """
        Generate vectors for a list of texts.
        
        Args:
            texts: List of text strings
            
        Returns:
            List of vector arrays
        """
        if not texts:
            return []
        
        vectors = self.embeddings.transform(texts)
        
        # Convert to list format if needed
        result = []
        for vector in vectors:
            if hasattr(vector, 'tolist'):
                result.append(vector.tolist())
            else:
                result.append(vector)
        
        return result
    
    def __del__(self):
        """Cleanup resources."""
        if self.embeddings:
            # Ensure no indexing or storage operations occur
            pass
