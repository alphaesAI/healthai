# pipeline/extractors/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator


class BaseExtractor(ABC):
    def __init__(self, name: str, connector):
        self.name = name
        self.connector = connector

    @abstractmethod
    def extract(self) -> Iterator[Dict[str, Any]]:
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        pass
