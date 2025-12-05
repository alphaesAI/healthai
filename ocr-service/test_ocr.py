#!/usr/bin/env python3
import requests
import os

# Test the OCR service with the ironman.jpg image
def test_ocr_service():
    # Image path
    image_path = "static/images/ironman.jpg"
    
    if not os.path.exists(image_path):
        print(f"Image not found: {image_path}")
        return
    
    # OCR service endpoint
    url = "http://localhost:8001/ocr/extract"
    
    try:
        # Open and send the image
        with open(image_path, "rb") as image_file:
            files = {"file": (image_path, image_file, "image/jpeg")}
            response = requests.post(url, files=files)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to OCR service. Make sure it's running on http://localhost:8001")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_ocr_service()
