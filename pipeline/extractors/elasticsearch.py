# pipeline/extractors/elasticsearch.py
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .registry import ExtractorRegistry


class ElasticsearchExtractor(BaseExtractor):
    def __init__(self, name: str, connector):
        super().__init__(name, connector)

    def extract(self, source: str = None, **kwargs) -> Iterator[Dict[str, Any]]:
        index = source or kwargs.get('index', '_all')
        query = kwargs.get('query', {"query": {"match_all": {}}})
        
        results = self.connector.search(index=index, body=query)
        for hit in results.get('hits', {}).get('hits', []):
            yield hit['_source']

    def test_connection(self) -> bool:
        return self.connector.test_connection()


ExtractorRegistry.register("elasticsearch", ElasticsearchExtractor)
