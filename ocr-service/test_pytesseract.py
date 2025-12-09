#!/usr/bin/env python3
import requests
import os

# Test the OCR service with pytesseract.png image
def test_pytesseract_image():
    # Test image path
    image_path = "ocr-service/static/images/tamil.png"
    
    if not os.path.exists(image_path):
        print(f"Image not found: {image_path}")
        return
    
    # OCR service endpoint
    url = "http://localhost:8001/ocr/extract"
    
    try:
        # Open and send the image
        with open(image_path, "rb") as image_file:
            files = {"file": (image_path, image_file, "image/png")}
            response = requests.post(url, files=files)
        
        print(f"Status Code: {response.status_code}")
        result = response.json()
        print(f"Extracted Text: '{result['data']['extracted_text']}'")
        print(f"Confidence: {result['data']['metadata']['confidence']}%")
        print(f"Word Count: {result['data']['metadata']['word_count']}")
        print(f"Character Count: {result['data']['metadata']['char_count']}")
        print(f"Full Response: {result}")
        
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to OCR service. Make sure it's running on http://localhost:8001")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_pytesseract_image()
