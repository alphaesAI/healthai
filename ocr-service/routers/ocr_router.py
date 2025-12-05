from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse
from service.ocr_service import OCRService

router = APIRouter(prefix="/ocr", tags=["OCR"])
ocr_service = OCRService()


@router.post("/extract")
async def extract_text_from_image(
    file: UploadFile = File(...),
    language: str = Query(default='en', description="Language code for OCR (e.g., 'en', 'tamil', 'hin')")
):
    """
    Extract text from an uploaded image file using OCR
    
    Args:
        file: Uploaded image file (JPG, PNG, PDF, etc.)
        language: Language code for OCR processing
        
    Returns:
        JSON response with extracted text and metadata
    """
    try:
        result = await ocr_service.extract_text_from_file(file, language)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/status")
async def get_ocr_engines_status():
    """Get status of available OCR engines"""
    try:
        status = ocr_service.get_engine_status()
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": status
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
