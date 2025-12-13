# pipeline/extractors/gmail.py

from typing import Iterator, Dict, Any, Optional
from .base import BaseExtractor
from .registry import ExtractorRegistry


class GmailExtractor(BaseExtractor):
    """
    Gmail extractor that fetches ONLY unread messages.

    Responsibilities:
    - Call Gmail connector endpoints
    - Apply extractor-level filtering logic
    - Yield fully fetched email objects
    """

    def __init__(self, name: str, connector):
        super().__init__(name=name, connector=connector)

    def extract(
        self,
        source: Optional[str] = None,
        mark_as_read: bool = False,
        **kwargs,
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract unread Gmail messages.

        Args:
            source: Not used (kept for interface compatibility)
            mark_as_read: Whether to mark messages as read after extraction

        Yields:
            Full Gmail message objects
        """

        # Gmail search query for unread messages
        query = "is:unread"

        messages = self.connector.list_messages(query=query)

        for msg in messages:
            message_id = msg.get("id")
            if not message_id:
                continue

            # Fetch full message
            message = self.connector.get_message(message_id)
            yield message

            # Optionally mark as read
            if mark_as_read:
                try:
                    self.connector.modify_labels(
                        message_id=message_id,
                        remove_labels=["UNREAD"],
                    )
                except Exception:
                    # Minimal error handling by design
                    pass

    def test_connection(self) -> bool:
        """Test Gmail connector availability."""
        return self.connector.test_connection()


# Register extractor
ExtractorRegistry.register("gmail", GmailExtractor)
