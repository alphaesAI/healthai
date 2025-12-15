import os
from typing import List, Dict, Any

from txtai.pipeline.data.textractor import Textractor
from .base import BaseTransformer
from .registry import TransformerRegistry


class DataTransformer(BaseTransformer):
    """
    Transforms extractor output into txtai-ready documents using Textractor.

    Input:
        List of dicts from extractors:
        {
            "metadata": {...},
            "html": "<html>...</html>",
            "attachments": ["/path/file.pdf"]
        }

    Output:
        List of dicts:
        {
            "id": str,
            "text": str,
            "tags": dict
        }
    """

    def __init__(
        self,
        name: str = "data_transformer",
        config: Dict[str, Any] | None = None
    ):
        super().__init__(name)

        config = config or {}

        # Extract textractor-specific config
        textractor_config = config.get("textractor", {})

        self.textractor = Textractor(**textractor_config)


    def transform(self, data: List[Dict[str, Any]], **kwargs) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for item in data:
            metadata = item.get("metadata", {})
            base_id = metadata.get("id", "unknown")

            tags = {
                "subject": metadata.get("subject"),
                "from": metadata.get("from"),
                "to": metadata.get("to"),
                "date": metadata.get("date"),
            }

            # ---------- HTML BODY ----------
            html = item.get("html")
            if html:
                texts = self._extract_text(html)
                for idx, text in enumerate(texts):
                    results.append({
                        "id": f"{base_id}_body_{idx}",
                        "text": text,
                        "tags": tags,
                    })

            # ---------- ATTACHMENTS ----------
            for filepath in item.get("attachments", []):
                if not os.path.exists(filepath):
                    continue

                texts = self._extract_text(filepath)
                file_id = os.path.basename(filepath)

                for idx, text in enumerate(texts):
                    results.append({
                        "id": f"{file_id}_{idx}",
                        "text": text,
                        "tags": tags,
                    })

        return results

    def _extract_text(self, source: str) -> List[str]:
        """
        Wrapper around Textractor.text() to normalize output.
        """
        output = self.textractor.text(source)

        if not output:
            return []

        if isinstance(output, str):
            return [output]

        return list(output)


# Register transformer
TransformerRegistry.register("data", DataTransformer)
