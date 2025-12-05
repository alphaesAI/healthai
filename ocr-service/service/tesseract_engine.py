from typing import Dict, Any
import io
from PIL import Image
import pytesseract
from .ocr_strategy import OCREngineStrategy


class TesseractEngine(OCREngineStrategy):
    """Tesseract OCR implementation for text extraction from images"""
    
    def __init__(self):
        try:
            # Test Tesseract availability
            pytesseract.get_tesseract_version()
            self.available = True
        except Exception as e:
            print(f"Tesseract initialization failed: {e}")
            self.available = False
    
    def extract_text(self, image_data: bytes, filename: str = None, language: str = 'en') -> Dict[str, Any]:
        """
        Extract text from image data using Tesseract OCR
        
        Args:
            image_data: Raw image bytes
            filename: Optional filename for the image
            language: Language code for OCR
            
        Returns:
            Dictionary containing extracted text and metadata
        """
        try:
            if not self.is_available():
                return {
                    "success": False,
                    "error": "Tesseract is not installed or not in PATH",
                    "text": "",
                    "metadata": {}
                }
            
            # Create a file-like object from image bytes
            image_stream = io.BytesIO(image_data)
            
            # Open and validate the image
            image = Image.open(image_stream)
            
            # Configure language for Tesseract
            lang_config = language if language != 'en' else 'eng'
            
            # Extract text using Tesseract OCR
            text_content = pytesseract.image_to_string(image, lang=lang_config)
            
            # Get additional OCR data for confidence
            try:
                ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT, lang=lang_config)
                avg_confidence = self._calculate_average_confidence(ocr_data)
            except:
                avg_confidence = 0.0
            
            # Calculate word count and character count
            words = text_content.split()
            word_count = len(words)
            char_count = len(text_content)
            
            return {
                "success": True,
                "text": text_content.strip(),
                "metadata": {
                    "filename": filename,
                    "engine": "tesseract",
                    "image_size": {
                        "width": image.width,
                        "height": image.height,
                        "mode": image.mode
                    },
                    "char_count": char_count,
                    "word_count": word_count,
                    "confidence": avg_confidence,
                    "language": language
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "text": "",
                "metadata": {"engine": "tesseract"}
            }
    
    def _calculate_average_confidence(self, ocr_data: dict) -> float:
        """Calculate average confidence from OCR data"""
        confidences = [int(conf) for conf in ocr_data['conf'] if int(conf) > 0]
        return sum(confidences) / len(confidences) if confidences else 0.0
    
    def is_available(self) -> bool:
        """Check if Tesseract is available"""
        return self.available
    
    def get_supported_languages(self) -> list:
        """Get list of supported languages"""
        # Common Tesseract language codes
        return ['eng', 'tam', 'hin', 'fra', 'deu', 'spa', 'ara', 'jpn', 'kor', 'chi_sim', 'chi_tra']
