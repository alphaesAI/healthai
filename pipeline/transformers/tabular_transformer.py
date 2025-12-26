# pipeline/transformers/tabular_transformer.py
from typing import Iterator, Tuple, Any, Dict
from pathlib import Path
import json
from .base import BaseTransformer
from txtai.pipeline.data.tabular import Tabular


class TabularTransformer(BaseTransformer):
    """
    Transformer for structured/tabular data using txtai Tabular pipeline.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.default_source = config.get('default_source')
        self.id_column = config.get('id_column', 'id')
        self.text_columns = config.get('text_columns', [])
        self.content_enabled = config.get('content_enabled', False)
        
        # Initialize txtai Tabular pipeline
        self.tabular = Tabular(
            idcolumn=self.id_column,
            textcolumns=self.text_columns,
            content=self.content_enabled
        )

    def transform(self) -> Iterator[Tuple[str, str, list]]:
        """
        Transform tabular data into (id, text, tags) tuples.
        
        Returns:
            Iterator of (id, text, tags) tuples
        """
        # Read extractor output
        data_dir = Path(__file__).parent.parent.parent / self.config.get('data_dir', 'data')
        extractors_subdir = self.config.get('extractors_subdir', 'extractors')
        file_path = data_dir / extractors_subdir / f'{self.config.get("source", self.default_source)}.json'
        
        if not file_path.exists():
            return
        
        with open(file_path, 'r') as f:
            records = json.load(f)
        
        # Use txtai Tabular pipeline to process data and yield results directly
        results = self.tabular(records)
        
        # Tabular already returns (id, text, None) tuples, just add tags
        for result in results:
            if isinstance(result, tuple) and len(result) == 3:
                record_id, text, _ = result
                tags = [f"source:{self.config.get('source', self.default_source)}"]
                yield (record_id, text, tags)
