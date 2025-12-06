"""
Mock vectors implementation for testing embeddings without external dependencies.
This follows SOLID principles and clean architecture.
"""

import numpy as np
from typing import List, Optional, Any
from ..base import Vectors


class MockVectors(Vectors):
    """
    Mock vectors implementation that generates random embeddings for testing.
    This follows the Single Responsibility Principle by only providing mock vectors.
    """
    
    def __init__(self, config, scoring=None, models=None):
        """Initialize mock vectors with configuration."""
        super().__init__(config, scoring, models)
        self.dimension = config.get('dimension', 384)
        
    def loadmodel(self, path):
        """Mock model loading - returns None since we don't need a real model."""
        return None
        
    def encode(self, data, category=None):
        """
        Generate mock embeddings for the input data.
        
        Args:
            data: Input text/documents
            category: Category parameter (ignored for mock)
            
        Returns:
            numpy array of random embeddings
        """
        # Generate random vectors for each document
        if isinstance(data, str):
            # Single document
            return np.random.rand(self.dimension).astype(np.float32)
        elif isinstance(data, list):
            # Multiple documents
            return np.random.rand(len(data), self.dimension).astype(np.float32)
        else:
            # Default case
            return np.random.rand(self.dimension).astype(np.float32)
