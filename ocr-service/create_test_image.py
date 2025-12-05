#!/usr/bin/env python3
from PIL import Image, ImageDraw, ImageFont
import os

# Create a simple test image with clear text
def create_test_image():
    # Create a white image
    img = Image.new('RGB', (400, 100), color='white')
    draw = ImageDraw.Draw(img)
    
    # Try to use a default font
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    except:
        font = ImageFont.load_default()
    
    # Add clear text
    text = "Hello OCR Test!"
    draw.text((50, 30), text, fill='black', font=font)
    
    # Save the image
    img.save('static/images/test_text.png')
    print("Test image created: static/images/test_text.png")

if __name__ == "__main__":
    create_test_image()
