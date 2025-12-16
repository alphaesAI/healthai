# pipeline/extractors/gmail.py

import base64
import os
from typing import Iterator, Dict, Any
from .base import BaseExtractor
from .registry import ExtractorRegistry


class GmailExtractor(BaseExtractor):
    """
    Gmail extractor that fetches messages based on configuration.

    Responsibilities:
    - Call Gmail connector endpoints
    - Apply extractor-level filtering logic
    - Yield fully fetched email objects
    - Extract and store attachments
    - Store output using writer
    """

    def __init__(self, name: str, connector, config: Dict[str, Any]):
        super().__init__(name=name, connector=connector, config=config)

    def extract(self) -> Iterator[Dict[str, Any]]:
        """
        Extract Gmail messages based on configuration.

        Yields:
            Full Gmail message objects with attachments
        """
        # Get configuration
        labels = self.config.get('labels', ['INBOX'])
        query = self.config.get('query', 'is:unread')
        mark_as_read = self.config.get('mark_as_read', False)
        batch_size = self.config.get('batch_size', 100)
        
        # Collect all messages for storage
        all_messages = []
        
        # Extract from each label
        for label in labels:
            # Build query with label
            label_query = f"label:{label} {query}".strip()
            
            messages = self.connector.list_messages(query=label_query)
            
            for msg in messages[:batch_size]:  # Limit by batch_size
                message_id = msg.get("id")
                if not message_id:
                    continue

                # Fetch full message
                message = self.connector.get_message(message_id)
                
                # Add label context
                message['_source_label'] = label
                
                # Extract and store attachments
                if 'payload' in message and 'parts' in message['payload']:
                    message['attachments'] = self._extract_and_store_attachments(message, message_id)
                
                all_messages.append(message)
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
        
        # Store all messages using writer
        from .writer import write
        write(all_messages, 'extractors/gmail.json')

    def _extract_and_store_attachments(self, message: Dict[str, Any], message_id: str) -> list:
        """Extract and store attachments from Gmail message."""
        attachments = []
        
        # Get project root directory (healthai project root)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        # Create attachment directory
        attachments_dir = os.path.join(project_root, "data", "attachments", message_id)
        os.makedirs(attachments_dir, exist_ok=True)
        
        def extract_parts(parts):
            for part in parts:
                if part.get('filename') and part.get('body', {}).get('attachmentId'):
                    # Get attachment data
                    attachment_id = part['body']['attachmentId']
                    filename = part['filename']
                    
                    try:
                        # Fetch attachment content
                        attachment_data = self.connector.get_attachment(message_id, attachment_id)
                        
                        # Decode base64 content
                        if 'data' in attachment_data:
                            content = base64.urlsafe_b64decode(attachment_data['data'])
                            
                            # Store attachment to file
                            attachment_path = os.path.join(attachments_dir, filename)
                            with open(attachment_path, 'wb') as f:
                                f.write(content)
                            
                            # Return relative path from project root
                            stored_path = os.path.relpath(attachment_path, project_root)
                            
                            attachments.append({
                                'filename': filename,
                                'attachment_id': attachment_id,
                                'size': len(content),
                                'mime_type': part.get('mimeType', 'application/octet-stream'),
                                'stored_path': stored_path
                            })
                    except Exception as e:
                        print(f"Error extracting attachment {filename}: {e}")
                        pass
                
                # Recursively check nested parts
                if 'parts' in part:
                    extract_parts(part['parts'])
        
        # Start extraction from payload parts
        extract_parts(message['payload']['parts'])
        
        return attachments


# Register extractor
ExtractorRegistry.register("gmail", GmailExtractor)
