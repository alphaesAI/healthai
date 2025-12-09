"""
Config-driven Gmail extractor for Sabi ETL pipeline.
"""

import base64
import os
import uuid
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
from .base import BaseExtractor

class GmailExtractor(BaseExtractor):
    """
    Extractor that pulls ALL Gmail messages and returns:
      - metadata JSON
      - cleaned HTML body
      - downloaded attachments (file paths)
    """

    def __init__(self, connector, config: dict = None):
        """
        Args:
            connector: GmailConnector instance
            config: dictionary, optional
                - download_dir: path to save attachments
        """
        super().__init__(connector, config or {})
        self.service = connector.service
        self.download_dir = (config or {}).get("download_dir", "downloads")
        os.makedirs(self.download_dir, exist_ok=True)
    # ────────────────────────────────────────────────
    # Main extract() API (required by BaseExtractor)
    # ────────────────────────────────────────────────

    def extract(self, service=None) -> List[Dict[str, Any]]:
        service = service or self.service
        results = []

        msg_list = service.users().messages().list(userId="me").execute()
        messages = msg_list.get("messages", [])
        if not messages:
            return []

        for msg in messages:
            msg_id = msg["id"]
            results.append(self._process_single_email(msg_id))  # FIXED

        return results


    # ────────────────────────────────────────────────
    # Fetch message list
    # ────────────────────────────────────────────────
    def _fetch_message_list(self, gmail_query: Optional[str]) -> List[str]:
        request = self.service.users().messages().list(
            userId="me",
            q=gmail_query
        )

        response = request.execute()
        messages = response.get("messages", [])

        if self.max_emails:
            messages = messages[: self.max_emails]

        return [msg["id"] for msg in messages]

    # ────────────────────────────────────────────────
    # Process a single message
    # ────────────────────────────────────────────────
    def _process_single_email(self, msg_id: str) -> Dict[str, Any]:
        metadata = self.get_metadata(msg_id)
        html = self.get_html(metadata)
        attachments = self.get_attachments(metadata)

        return {
            "id": msg_id,
            "metadata": metadata,
            "html": html,
            "attachments": attachments,
        }

    # ────────────────────────────────────────────────
    # Metadata extraction
    # ────────────────────────────────────────────────
    def get_metadata(self, msg_id: str) -> Dict[str, Any]:
        message = self.service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full"
        ).execute()

        payload = message.get("payload", {})
        headers = payload.get("headers", [])

        metadata = {
            "id": msg_id,
            "subject": self._get_header(headers, "Subject"),
            "from": self._get_header(headers, "From"),
            "to": self._get_header(headers, "To"),
            "date": self._get_header(headers, "Date"),
            "snippet": message.get("snippet", ""),
            "parts": payload.get("parts", [])
        }

        return metadata

    def _get_header(self, headers, name):
        for h in headers:
            if h["name"].lower() == name.lower():
                return h["value"]
        return None

    # ────────────────────────────────────────────────
    # HTML / text extraction
    # ────────────────────────────────────────────────
    def get_html(self, metadata: Dict[str, Any]) -> str:
        parts = metadata.get("parts", [])
        html_data = None

        for part in parts:
            mime = part.get("mimeType", "")

            if mime == "text/html":
                html_data = part["body"].get("data")
                break

            if mime == "text/plain" and html_data is None:
                html_data = self._convert_text_to_html(part["body"].get("data"))

        if not html_data:
            return ""

        decoded = base64.urlsafe_b64decode(html_data).decode("utf-8", errors="ignore")
        return self._clean_html(decoded)

    def _convert_text_to_html(self, encoded_text):
        if not encoded_text:
            return ""
        text = base64.urlsafe_b64decode(encoded_text).decode("utf-8", errors="ignore")
        return f"<pre>{text}</pre>"

    def _clean_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        return str(soup)

    # ────────────────────────────────────────────────
    # Attachments
    # ────────────────────────────────────────────────
    def get_attachments(self, metadata: Dict[str, Any]) -> List[str]:
        parts = metadata.get("parts", [])
        file_paths = []

        for part in parts:
            filename = part.get("filename")
            body = part.get("body", {})

            if not filename or "attachmentId" not in body:
                continue

            attachment_id = body["attachmentId"]

            file_data = self.service.users().messages().attachments().get(
                userId="me",
                messageId=metadata["id"],
                id=attachment_id
            ).execute()

            data = file_data.get("data")
            if not data:
                continue

            decoded = base64.urlsafe_b64decode(data)

            file_path = os.path.join(
                self.download_dir, f"{uuid.uuid4()}-{filename}"
            )

            with open(file_path, "wb") as f:
                f.write(decoded)

            file_paths.append(file_path)

        return file_paths

    # ────────────────────────────────────────────────
    # Schema (emails do not have schema)
    # ────────────────────────────────────────────────
    def get_schema(self, source: str = None) -> Dict[str, Any]:
        """
        Gmail does not have a strict schema. Returning a fixed metadata schema.
        """
        return {
            "fields": [
                "id",
                "subject",
                "from",
                "to",
                "date",
                "snippet",
                "html",
                "attachments"
            ]
        }


# Example view of results
# results = [
#   {
#     "id": "abc123",
#     "metadata": {...},
#     "html": "<html>...</html>",
#     "attachments": ["downloads/xxx.pdf"]
#   },
#   {
#     "id": "def456",
#     "metadata": {...},
#     "html": "<html>...</html>",
#     "attachments": []
#   }
# ]
