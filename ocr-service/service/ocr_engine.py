from typing import Dict, Any
from .ocr_strategy import OCREngineStrategy, OCREngineFactory


class OCREngine:
    """Main OCR engine using strategy pattern with fallback support"""
    
    def __init__(self, primary_engine: str = 'paddleocr', fallback_engine: str = 'tesseract'):
        """
        Initialize OCR engine with primary and fallback options
        
        Args:
            primary_engine: Primary OCR engine to use
            fallback_engine: Fallback OCR engine if primary fails
        """
        self.primary_engine = OCREngineFactory.create_engine(primary_engine)
        self.fallback_engine = OCREngineFactory.create_engine(fallback_engine)
    
    def extract_text_from_image(self, image_data: bytes, filename: str = None, language: str = 'en') -> dict:
        """
        Extract text from image data using primary OCR engine with fallback
        
        Args:
            image_data: Raw image bytes
            filename: Optional filename for the image
            language: Language code for OCR
            
        Returns:
            Dictionary containing extracted text and metadata
        """
        # Try primary engine first
        if self.primary_engine.is_available():
            result = self.primary_engine.extract_text(image_data, filename, language)
            if result["success"] and result["text"].strip():
                return result
        
        # Fallback to secondary engine
        if self.fallback_engine.is_available():
            result = self.fallback_engine.extract_text(image_data, filename, language)
            if result["success"]:
                # Add fallback info to metadata
                result["metadata"]["fallback_used"] = True
                result["metadata"]["primary_engine"] = self.primary_engine.__class__.__name__
                return result
        
        # Both engines failed
        return {
            "success": False,
            "error": "Both OCR engines failed or are unavailable",
            "text": "",
            "metadata": {
                "primary_engine": self.primary_engine.__class__.__name__,
                "fallback_engine": self.fallback_engine.__class__.__name__
            }
        }
    
    def get_engine_status(self) -> dict:
        """Get status of both OCR engines"""
        return {
            "primary_engine": {
                "name": self.primary_engine.__class__.__name__,
                "available": self.primary_engine.is_available(),
                "supported_languages": self.primary_engine.get_supported_languages()
            },
            "fallback_engine": {
                "name": self.fallback_engine.__class__.__name__,
                "available": self.fallback_engine.is_available(),
                "supported_languages": self.fallback_engine.get_supported_languages()
            }
        }
