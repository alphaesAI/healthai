from abc import ABC, abstractmethod
from typing import Dict, Any


class OCREngineStrategy(ABC):
    """Abstract base class for OCR engine strategies"""
    
    @abstractmethod
    def extract_text(self, image_data: bytes, filename: str = None, language: str = 'en') -> Dict[str, Any]:
        """
        Extract text from image data
        
        Args:
            image_data: Raw image bytes
            filename: Optional filename for the image
            language: Language code for OCR
            
        Returns:
            Dictionary containing extracted text and metadata
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the OCR engine is available and properly configured"""
        pass
    
    @abstractmethod
    def get_supported_languages(self) -> list:
        """Get list of supported languages"""
        pass


class OCREngineFactory:
    """Factory for creating OCR engine instances"""
    
    @staticmethod
    def create_engine(engine_type: str) -> OCREngineStrategy:
        """
        Create OCR engine instance based on type
        
        Args:
            engine_type: Type of OCR engine ('paddleocr', 'tesseract')
            
        Returns:
            OCR engine strategy instance
        """
        if engine_type.lower() == 'paddleocr':
            from .paddleocr_engine import PaddleOCREngine
            return PaddleOCREngine()
        elif engine_type.lower() == 'tesseract':
            from .tesseract_engine import TesseractEngine
            return TesseractEngine()
        else:
            raise ValueError(f"Unsupported OCR engine type: {engine_type}")
