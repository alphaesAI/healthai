# pipeline/transformers/json.py
from typing import Any, List, Dict
import json
import pandas as pd

from .base import BaseTransformer
from .registry import TransformerRegistry


class JsonTransformer(BaseTransformer):
    """
    Transforms DataFrame or list[dict] into JSON.
    """

    def transform(self, data: Any, **kwargs) -> Any:
        if isinstance(data, pd.DataFrame):
            return data.to_dict(orient="records")

        if isinstance(data, list):
            return json.loads(json.dumps(data))

        raise TypeError("Unsupported input type for JsonTransformer")

    def get_output_schema(self) -> Dict[str, str]:
        return {"type": "json", "structure": "list[dict]"}


TransformerRegistry.register("json", JsonTransformer)
