# pipeline/transformers/document_transformer.py
from typing import Iterator, Tuple, Any, Dict
from pathlib import Path
import json
from .base import BaseTransformer
from txtai.pipeline.data.textractor import Textractor


class DocumentTransformer(BaseTransformer):
    """
    Transformer for unstructured data using txtai Textractor pipeline.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.default_source = config.get('default_source')
        self.include_attachments = config.get('include_attachments', True)
        self.attachment_extensions = config.get('attachment_extensions', ['.pdf', '.txt', '.doc', '.docx'])
        self.chunk_separator = config.get('chunk_separator', '_chunk_')
        self.attachment_separator = config.get('attachment_separator', '_attachment_')
        self.subject_length_limit = config.get('subject_length_limit', 50)
        self.default_tag = config.get('default_tag')
        self.document_placeholder = config.get('document_placeholder', '[Document: {filename}]')
        
        # Initialize txtai Textractor pipeline with all parameters
        textractor_config = config.get('textractor', {})
        segmentation_config = config.get('segmentation', {})
        
        # Pass all parameters to Textractor (it inherits from Segmentation)
        all_config = {**textractor_config, **segmentation_config}
        self.textractor = Textractor(**all_config)

    def transform(self) -> Iterator[Tuple[str, str, list]]:
        """
        Transform unstructured data into (id, text, tags) tuples.
        
        Returns:
            Iterator of (id, text, tags) tuples
        """
        # Read extractor output
        data_dir = Path(__file__).parent.parent.parent / self.config.get('data_dir', 'data')
        extractors_subdir = self.config.get('extractors_subdir', 'extractors')
        file_path = data_dir / extractors_subdir / f'{self.config.get("source", self.default_source)}.json'
        
        if not file_path.exists():
            return
        
        with open(file_path, 'r') as f:
            records = json.load(f)
        
        for record in records:
            record_id = record.get('id', '')
            
            # Process email body
            body = record.get('body', '')
            if body:
                # Use txtai Textractor as callable to get chunks (not text() method)
                processed_chunks = self.textractor(body)
                
                # Textractor returns list of chunks when segmentation is enabled
                if isinstance(processed_chunks, list):
                    for i, chunk in enumerate(processed_chunks):
                        chunk_id = f"{record_id}{self.chunk_separator}{i}"
                        tags = self._extract_tags(record)
                        yield (chunk_id, chunk, tags)
                else:
                    tags = self._extract_tags(record)
                    yield (record_id, processed_chunks, tags)
            
            # Process attachments if enabled
            if self.include_attachments and 'attachments' in record:
                for attachment in record['attachments']:
                    attachment_id = f"{record_id}{self.attachment_separator}{attachment['filename']}"
                    attachment_text = self._extract_attachment_text(attachment)
                    
                    if attachment_text:
                        # Use txtai Textractor as callable to get chunks
                        processed_chunks = self.textractor(attachment_text)
                        
                        if isinstance(processed_chunks, list):
                            for i, chunk in enumerate(processed_chunks):
                                chunk_id = f"{attachment_id}{self.chunk_separator}{i}"
                                tags = self._extract_tags(record) + [f"attachment:{attachment['filename']}"]
                                yield (chunk_id, chunk, tags)
                        else:
                            tags = self._extract_tags(record) + [f"attachment:{attachment['filename']}"]
                            yield (attachment_id, processed_chunks, tags)

    def _extract_tags(self, record: Dict[str, Any]) -> list:
        """Extract tags from record metadata."""
        tags = [self.default_tag] if self.default_tag else []
        
        # Add tags from metadata
        metadata = record.get('metadata', {})
        if 'subject' in metadata:
            subject = metadata['subject']
            if len(subject) > self.subject_length_limit:
                subject = subject[:self.subject_length_limit]
            tags.append(f"subject:{subject}")
        if 'from' in metadata:
            tags.append(f"from:{metadata['from']}")
        if 'labels' in metadata:
            for label in metadata['labels']:
                tags.append(f"label:{label}")
        
        return tags

    def _extract_attachment_text(self, attachment: Dict[str, Any]) -> str:
        """Extract text from attachment file."""
        data_dir = Path(__file__).parent.parent.parent / self.config.get('data_dir', 'data')
        attachment_path = data_dir / attachment['path']
        
        if not attachment_path.exists():
            return ""
        
        # Simple text extraction based on file extension
        extension = Path(attachment['filename']).suffix.lower()
        
        if extension == '.txt':
            try:
                with open(attachment_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return ""
        
        # For PDF and other formats, return placeholder for now
        # OCR integration would go here in future
        if extension in self.attachment_extensions and extension in ['.pdf', '.doc', '.docx']:
            return self.document_placeholder.format(filename=attachment['filename'])
        
        return ""
