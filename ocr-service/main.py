from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.ocr_router import router as ocr_router

# Create FastAPI app
app = FastAPI(
    title="OCR Service",
    description="Microservice for extracting text from images using OCR",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ocr_router)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "OCR Service",
        "status": "running",
        "version": "1.0.0",
        "endpoints": {
            "health": "/",
            "docs": "/docs",
            "ocr_extract": "/ocr/extract"
        }
    }


@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "service": "ocr-service",
        "version": "1.0.0"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
