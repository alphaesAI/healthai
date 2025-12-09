import os
from typing import List, Dict, Tuple, Any
from ..pipeline.data.textractor import Textractor


class DataTransformer:
    """
    Transformer that converts extractor output into txtai-ready tuples using Textractor.
    It unpacks extractor output and lets Textractor handle HTML/file processing.
    """

    def __init__(self, textractor_config: Dict[str, Any] = None):
        """
        Args:
            textractor_config: Config dictionary passed directly to Textractor
        """
        textractor_config = textractor_config or {}
        self.textractor = Textractor(**textractor_config)

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert extractor output into embeddings-ready dictionaries.

        Args:
            data: List of extractor dictionaries with keys:
                  - metadata: dict
                  - html: email body string
                  - attachments: list of file paths

        Returns:
            List of dictionaries: {"id": id, "text": text, "tags": tags}
        """
        results: List[Dict[str, Any]] = []

        for item in data:
            metadata = item.get("metadata", {})
            email_id = metadata.get("id", "unknown")
            tags = {
                "subject": metadata.get("subject"),
                "from": metadata.get("from"),
                "to": metadata.get("to"),
                "date": metadata.get("date")
            }

            # Process email body
            html = item.get("html")
            if html:
                segments = self.textractor.text(html)
                if isinstance(segments, str):
                    results.append({"id": email_id, "text": segments, "tags": tags})
                else:
                    for seg in segments:
                        results.append({"id": email_id, "text": seg, "tags": tags})

            # Process attachments (file paths)
            for filepath in item.get("attachments", []):
                if os.path.exists(filepath):
                    segments = self.textractor.text(filepath)
                    file_id = os.path.basename(filepath)
                    if isinstance(segments, str):
                        results.append({"id": file_id, "text": segments, "tags": tags})
                    else:
                        for seg in segments:
                            results.append({"id": file_id, "text": seg, "tags": tags})

        return results
