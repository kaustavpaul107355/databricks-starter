import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import os

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# --- FastAPI App Setup ---
app = FastAPI(
    title="Zerobus Delta App",
    description="FastAPI server for processing payloads and writing to Delta tables via Zerobus",
    version="1.0.0"
)

# Add CORS middleware to allow web UI to make requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for the web UI
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# --- Pydantic Models ---
class PayloadItem(BaseModel):
    """Individual item in the payload"""
    id: Optional[str] = Field(None, description="Optional ID for the item")
    data: Dict[str, Any] = Field(..., description="The actual data payload")
    timestamp: Optional[datetime] = Field(None, description="Optional timestamp")
    category: Optional[str] = Field(None, description="Optional category for routing")

class IncomingPayload(BaseModel):
    """Main payload structure for incoming requests"""
    items: List[PayloadItem] = Field(..., description="List of payload items to process")
    source: Optional[str] = Field(None, description="Source system identifier")
    batch_id: Optional[str] = Field(None, description="Batch identifier")
    
    @field_validator('items')
    @classmethod
    def validate_items_not_empty(cls, v):
        if not v:
            raise ValueError('Items list cannot be empty')
        return v

class TransformedItem(BaseModel):
    """Transformed item structure"""
    original_id: Optional[str]
    processed_id: str
    data: Dict[str, Any]
    timestamp: datetime
    category: str
    source: str
    processing_status: str = "processed"

class ProcessingResponse(BaseModel):
    """Response structure for successful processing"""
    status: str = "success"
    message: str
    batch_id: str
    processed_count: int
    items_processed: List[TransformedItem]

class ErrorResponse(BaseModel):
    """Error response structure"""
    status: str = "error"
    message: str
    error_code: str
    details: Optional[Dict[str, Any]] = None

# --- Helper Functions ---
def generate_batch_id() -> str:
    """Generate a unique batch ID"""
    return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def transform_payload_item(item: PayloadItem, source: str, batch_id: str) -> TransformedItem:
    """Transform a single payload item"""
    # Add processing timestamp
    processing_time = datetime.now()
    
    # Generate processed ID if not provided
    processed_id = item.id if item.id else f"item_{uuid.uuid4().hex[:8]}"
    
    # Determine category (example logic - can be customized)
    category = item.category if item.category else "default"
    
    # Add metadata to the data
    enhanced_data = {
        **item.data,
        "processed_at": processing_time.isoformat(),
        "batch_id": batch_id,
        "processing_metadata": {
            "original_timestamp": item.timestamp.isoformat() if item.timestamp else None,
            "processing_duration_ms": 0  # Placeholder for actual processing time
        }
    }
    
    return TransformedItem(
        original_id=item.id,
        processed_id=processed_id,
        data=enhanced_data,
        timestamp=processing_time,
        category=category,
        source=source or "unknown"
    )

def validate_and_split_payload(payload: IncomingPayload) -> Dict[str, List[TransformedItem]]:
    """
    Validate and split payload into different categories/tables
    Returns a dictionary where keys are table names and values are lists of items
    """
    batch_id = payload.batch_id or generate_batch_id()
    source = payload.source or "api"
    
    # Dictionary to hold items by category (which will map to different tables)
    categorized_items = {}
    
    for item in payload.items:
        try:
            # Transform the item
            transformed_item = transform_payload_item(item, source, batch_id)
            
            # Group by category (this determines which table it goes to)
            category = transformed_item.category
            if category not in categorized_items:
                categorized_items[category] = []
            
            categorized_items[category].append(transformed_item)
            
        except Exception as e:
            logger.error(f"Error transforming item {item.id}: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to transform item {item.id}: {str(e)}"
            )
    
    return categorized_items

# --- API Routes ---
@app.get("/")
async def serve_ui():
    """Serve the web UI"""
    static_file = os.path.join(static_dir, "index.html")
    if os.path.exists(static_file):
        return FileResponse(static_file)
    else:
        # Fallback API response if no UI is available
        return {
            "service": "zerobus-delta-app", 
            "status": "running",
            "version": "1.0.0",
            "endpoints": {
                "health": "/health",
                "docs": "/docs",
                "process": "/api/v1/process",
                "process_simple": "/api/v1/process-simple"
            },
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api")
async def api_info():
    """API information endpoint"""
    return {
        "service": "zerobus-delta-app", 
        "status": "running",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "process": "/api/v1/process",
            "process_simple": "/api/v1/process-simple"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "zerobus-delta-app", "timestamp": datetime.now().isoformat()}

@app.get("/debug")
async def debug_info():
    """Debug endpoint to help troubleshoot deployment issues"""
    return {
        "service": "zerobus-delta-app",
        "status": "debug",
        "python_version": "3.x",
        "fastapi_version": "0.109.0",
        "available_routes": [
            {"path": "/", "method": "GET", "name": "root"},
            {"path": "/health", "method": "GET", "name": "health_check"},
            {"path": "/debug", "method": "GET", "name": "debug_info"},
            {"path": "/api/v1/process", "method": "POST", "name": "process_payload"},
            {"path": "/api/v1/process-simple", "method": "POST", "name": "process_simple_payload"},
            {"path": "/docs", "method": "GET", "name": "swagger_ui"},
            {"path": "/redoc", "method": "GET", "name": "redoc"}
        ],
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/process", response_model=ProcessingResponse)
async def process_payload(payload: IncomingPayload, request: Request):
    """
    Main endpoint to receive and process payloads
    
    This endpoint:
    1. Receives JSON payloads
    2. Validates the payload structure
    3. Transforms and splits the payload into different categories
    4. Prepares data for writing to Delta tables (via Zerobus in next phase)
    5. Returns success/error responses
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"Received payload processing request from {request.client.host}")
        logger.info(f"Payload contains {len(payload.items)} items")
        
        # Validate and transform the payload
        categorized_items = validate_and_split_payload(payload)
        
        # Log the distribution of items across categories
        for category, items in categorized_items.items():
            logger.info(f"Category '{category}': {len(items)} items")
        
        # Flatten all items for response
        all_processed_items = []
        for items in categorized_items.values():
            all_processed_items.extend(items)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Update processing metadata
        for item in all_processed_items:
            item.data["processing_metadata"]["processing_duration_ms"] = processing_time
        
        batch_id = payload.batch_id or generate_batch_id()
        
        # TODO: In next phase, this is where we'll integrate Zerobus
        # to write the categorized_items to their respective Delta tables
        
        logger.info(f"Successfully processed {len(all_processed_items)} items in {processing_time:.2f}ms")
        
        return ProcessingResponse(
            message=f"Successfully processed {len(all_processed_items)} items across {len(categorized_items)} categories",
            batch_id=batch_id,
            processed_count=len(all_processed_items),
            items_processed=all_processed_items
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions (they're already properly formatted)
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing payload: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/api/v1/process-simple")
async def process_simple_payload(payload: Dict[str, Any], request: Request):
    """
    Simplified endpoint for basic payload processing
    Accepts any JSON structure and applies basic transformations
    """
    try:
        logger.info(f"Received simple payload from {request.client.host}")
        
        # Add processing metadata
        processed_payload = {
            **payload,
            "processed_at": datetime.now().isoformat(),
            "processed_by": "zerobus-delta-app",
            "batch_id": generate_batch_id()
        }
        
        # TODO: Add Zerobus integration here for simple payloads
        
        return {
            "status": "success",
            "message": "Simple payload processed successfully",
            "data": processed_payload
        }
        
    except Exception as e:
        logger.error(f"Error processing simple payload: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

# --- Exception Handlers ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            message=exc.detail,
            error_code=f"HTTP_{exc.status_code}",
            details={"path": str(request.url), "method": request.method}
        ).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """General exception handler for unexpected errors"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="Internal server error",
            error_code="INTERNAL_ERROR",
            details={"path": str(request.url), "method": request.method}
        ).dict()
    )

# --- Startup Event ---
@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    logger.info("Zerobus Delta App starting up...")
    logger.info("FastAPI server ready to process payloads")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event"""
    logger.info("Zerobus Delta App shutting down...")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
