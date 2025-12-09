from typing import Dict, Any, Optional
from fastapi import UploadFile, HTTPException
import os
from .ocr_engine import OCREngine


class OCRService:
    """Service layer for OCR operations"""
    
    def __init__(self):
        self.ocr_engine = OCREngine(primary_engine='paddleocr', fallback_engine='tesseract')
        self.max_file_size = 50 * 1024 * 1024  # 50MB
        self.allowed_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif', '.pdf'}
    
    async def extract_text_from_file(self, file: UploadFile, language: str = 'en') -> Dict[str, Any]:
        """
        Extract text from uploaded file with validation
        
        Args:
            file: Uploaded file instance
            language: Language code for OCR (default: 'en')
            
        Returns:
            Dictionary with extracted text and metadata
            
        Raises:
            HTTPException: For validation or processing errors
        """
        # Validate file size
        content = await file.read()
        file_size = len(content)
        
        if file_size > self.max_file_size:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size is {self.max_file_size // (1024*1024)}MB"
            )
        
        # Validate file type
        file_extension = os.path.splitext(file.filename)[1].lower()
        
        if file_extension not in self.allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type. Allowed types: {', '.join(self.allowed_extensions)}"
            )
        
        # Perform OCR using engine with fallback
        result = self.ocr_engine.extract_text_from_image(content, file.filename, language)
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=f"OCR processing failed: {result['error']}"
            )
        
        return {
            "filename": file.filename,
            "file_size": file_size,
            "extracted_text": result["text"],
            "metadata": result["metadata"]
        }
    
    def get_engine_status(self) -> Dict[str, Any]:
        """Get status of available OCR engines"""
        return self.ocr_engine.get_engine_status()
