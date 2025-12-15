# pipeline/extractors/elasticsearch.py
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .registry import ExtractorRegistry


class ElasticsearchExtractor(BaseExtractor):
    def __init__(self, name: str, connector, config: Dict[str, Any]):
        super().__init__(name, connector, config)

    def extract(self) -> Iterator[Dict[str, Any]]:
        """Extract documents from Elasticsearch based on configuration."""
        index = self.config.get('index', '_all')
        query = self.config.get('query', {"query": {"match_all": {}}})
        size = self.config.get('size', 100)
        
        # Add size to query if not present
        if 'size' not in query:
            query['size'] = size
        
        results = self.connector.search(index=index, body=query)
        for hit in results.get('hits', {}).get('hits', []):
            yield hit['_source']


ExtractorRegistry.register("elasticsearch", ElasticsearchExtractor)
