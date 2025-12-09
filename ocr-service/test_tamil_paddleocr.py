#!/usr/bin/env python3
import requests
import os

# Test the OCR service with Tamil image using PaddleOCR
def test_tamil_paddleocr():
    # Test image path
    image_path = "/media/logi/New Volume/github/logibsc/sabi-vaayaadi/ocr-service/static/images/tamil.png"
    
    if not os.path.exists(image_path):
        print(f"Image not found: {image_path}")
        return
    
    # OCR service endpoint
    url = "http://localhost:8001/ocr/extract"
    
    try:
        # Open and send the image with Tamil language parameter
        with open(image_path, "rb") as image_file:
            files = {"file": (image_path, image_file, "image/png")}
            params = {"language": "tamil"}
            response = requests.post(url, files=files, params=params)
        
        print(f"Status Code: {response.status_code}")
        result = response.json()
        print(f"Extracted Text: '{result['data']['extracted_text']}'")
        print(f"Engine Used: {result['data']['metadata'].get('engine', 'unknown')}")
        print(f"Confidence: {result['data']['metadata'].get('confidence', 'N/A')}%")
        print(f"Fallback Used: {result['data']['metadata'].get('fallback_used', False)}")
        print(f"Word Count: {result['data']['metadata']['word_count']}")
        print(f"Character Count: {result['data']['metadata']['char_count']}")
        print(f"Full Response: {result}")
        
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to OCR service. Make sure it's running on http://localhost:8001")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_tamil_paddleocr()
