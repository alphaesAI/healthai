from typing import Dict, Any
import io
from PIL import Image
from paddleocr import PaddleOCR
import numpy as np
from .ocr_strategy import OCREngineStrategy


class PaddleOCREngine(OCREngineStrategy):
    """PaddleOCR implementation for text extraction from images"""
    
    def __init__(self):
        try:
            # Initialize PaddleOCR with use_angle_cls=True for better text detection
            self.ocr = PaddleOCR(use_angle_cls=True, lang='en')
            self.available = True
        except Exception as e:
            print(f"PaddleOCR initialization failed: {e}")
            self.available = False
    
    def extract_text(self, image_data: bytes, filename: str = None, language: str = 'en') -> Dict[str, Any]:
        """
        Extract text from image data using PaddleOCR
        
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
                    "error": "PaddleOCR is not available",
                    "text": "",
                    "metadata": {}
                }
            
            # Convert bytes to PIL Image
            image_stream = io.BytesIO(image_data)
            image = Image.open(image_stream)
            
            # Convert PIL Image to numpy array (PaddleOCR expects numpy array)
            image_array = np.array(image)
            
            # Perform OCR
            result = self.ocr.ocr(image_array, cls=True)
            
            # Extract text from results
            extracted_texts = []
            confidences = []
            
            if result and result[0]:
                for line in result[0]:
                    if line and len(line) > 1:
                        text = line[1][0]  # Extract text
                        confidence = line[1][1]  # Extract confidence
                        extracted_texts.append(text)
                        confidences.append(confidence)
            
            # Combine all extracted text
            full_text = '\n'.join(extracted_texts)
            
            # Calculate average confidence
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            return {
                "success": True,
                "text": full_text.strip(),
                "metadata": {
                    "filename": filename,
                    "engine": "paddleocr",
                    "image_size": {
                        "width": image.width,
                        "height": image.height,
                        "mode": image.mode
                    },
                    "char_count": len(full_text),
                    "word_count": len(full_text.split()) if full_text.strip() else 0,
                    "confidence": round(avg_confidence * 100, 2),  # Convert to percentage
                    "detected_lines": len(extracted_texts),
                    "language": language
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "text": "",
                "metadata": {"engine": "paddleocr"}
            }
    
    def is_available(self) -> bool:
        """Check if PaddleOCR is available"""
        return self.available
    
    def get_supported_languages(self) -> list:
        """Get list of supported languages"""
        # PaddleOCR supports many languages, returning common ones
        return ['en', 'ch', 'japan', 'korean', 'fr', 'german', 'spanish', 'arabic', 'tamil', 'hindi']
