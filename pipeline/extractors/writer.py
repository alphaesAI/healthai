# pipeline/extractors/writer.py
import json
import base64
from pathlib import Path
from typing import Any, Dict, Union


def write(data: Union[Dict[str, Any], list], path: str) -> None:
    """Write extractor output to project storage as JSON.
    
    Args:
        data: Data to write (dict or list)
        path: Output file path (relative to project root)
    """
    # Get project root directory
    project_root = Path(__file__).parent.parent.parent
    
    # Create full path relative to project root
    full_path = project_root / "data" / path
    
    # Create directory if it doesn't exist
    full_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Handle Gmail messages with attachments
    if path == 'extractors/gmail.json' and isinstance(data, list):
        data = _process_gmail_attachments(data, project_root)
    
    # Write data as JSON
    with open(full_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)


def _process_gmail_attachments(messages: list, project_root: Path) -> list:
    """Process and store attachments from Gmail messages."""
    processed_messages = []
    
    for message in messages:
        processed_message = message.copy()
        
        # Extract and store attachments if present
        if 'payload' in message and 'parts' in message['payload']:
            attachments = _extract_attachments_from_parts(message['payload']['parts'], message['id'], project_root)
            if attachments:
                processed_message['attachments'] = attachments
        
        processed_messages.append(processed_message)
    
    return processed_messages


def _extract_attachments_from_parts(parts: list, message_id: str, project_root: Path) -> list:
    """Extract attachments from message parts."""
    attachments = []
    
    for part in parts:
        if part.get('filename') and part.get('body', {}).get('attachmentId'):
            filename = part['filename']
            attachment_id = part['body']['attachmentId']
            
            # Create attachment directory
            attachments_dir = project_root / "data" / "attachments" / message_id
            attachments_dir.mkdir(parents=True, exist_ok=True)
            
            # Store attachment info
            attachment_info = {
                'filename': filename,
                'attachment_id': attachment_id,
                'mime_type': part.get('mimeType', 'application/octet-stream'),
                'stored_path': f"data/attachments/{message_id}/{filename}"
            }
            attachments.append(attachment_info)
        
        # Recursively check nested parts
        if 'parts' in part:
            nested_attachments = _extract_attachments_from_parts(part['parts'], message_id, project_root)
            attachments.extend(nested_attachments)
    
    return attachments
