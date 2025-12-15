# pipeline/transformers/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def transform(self, data: Any, **kwargs) -> Any:
        """Transform input data into output data"""
        raise NotImplementedError

    def test_transform(self) -> bool:
        """Basic sanity check"""
        return True

    def get_transform_stats(self) -> Dict[str, Any]:
        """Optional stats"""
        return {
            "transformer": self.name,
            "status": "ok",
        }
