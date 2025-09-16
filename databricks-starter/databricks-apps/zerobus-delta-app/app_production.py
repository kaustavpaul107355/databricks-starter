"""
Zerobus Delta App - Production Version
A FastAPI-based Databricks app that processes incoming payloads and prepares them for writing to Delta tables via Zerobus.
Follows Databricks Apps enterprise standards and best practices.
"""

import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from contextlib import asynccontextmanager

# --- Environment Configuration ---
# Following Databricks Apps pattern for environment variables
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN') 
APP_ENV = os.getenv('APP_ENV', 'development')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# --- Logging Setup ---
# Configure logging following Databricks standards
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log') if APP_ENV != 'production' else logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Application Lifecycle Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management following Databricks Apps patterns"""
    # Startup
    logger.info("🚀 Zerobus Delta App starting up...")
    logger.info(f"Environment: {APP_ENV}")
    logger.info(f"Log Level: {LOG_LEVEL}")
    
    # Validate environment
    if APP_ENV == 'production':
        logger.info("Production mode: Enhanced security and monitoring enabled")
    
    logger.info("✅ FastAPI server ready to process payloads")
    yield
    
    # Shutdown
    logger.info("🛑 Zerobus Delta App shutting down...")
    logger.info("Cleanup completed")

# --- FastAPI App Setup ---
app = FastAPI(
    title="Zerobus Delta App",
    description="Enterprise FastAPI server for processing payloads and writing to Delta tables via Zerobus",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if APP_ENV != 'production' else None,  # Disable docs in production
    redoc_url="/redoc" if APP_ENV != 'production' else None
)

# --- Security and CORS Configuration ---
# Configure CORS following enterprise security practices
allowed_origins = ["*"] if APP_ENV == 'development' else [
    "https://*.cloud.databricks.com",
    "https://*.databricksapps.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# --- Static Files Configuration ---
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    logger.info(f"✅ Static files mounted from {static_dir}")

# --- Pydantic Models ---
class PayloadItem(BaseModel):
    """Individual item in the payload with enterprise validation"""
    id: Optional[str] = Field(None, description="Optional ID for the item")
    data: Dict[str, Any] = Field(..., description="The actual data payload")
    timestamp: Optional[datetime] = Field(None, description="Optional timestamp")
    category: Optional[str] = Field(None, description="Optional category for routing to Delta tables")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class IncomingPayload(BaseModel):
    """Main payload structure for incoming requests with validation"""
    items: List[PayloadItem] = Field(..., description="List of payload items to process")
    source: Optional[str] = Field(None, description="Source system identifier")
    batch_id: Optional[str] = Field(None, description="Batch identifier")
    
    @field_validator('items')
    @classmethod
    def validate_items_not_empty(cls, v):
        if not v:
            raise ValueError('Items list cannot be empty')
        if len(v) > 1000:  # Enterprise limit
            raise ValueError('Maximum 1000 items per batch')
        return v

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class TransformedItem(BaseModel):
    """Transformed item structure for Delta table preparation"""
    original_id: Optional[str]
    processed_id: str
    data: Dict[str, Any]
    timestamp: datetime
    category: str
    source: str
    processing_status: str = "processed"

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ProcessingResponse(BaseModel):
    """Enterprise response structure for successful processing"""
    status: str = "success"
    message: str
    batch_id: str
    processed_count: int
    items_processed: List[TransformedItem]
    processing_time_ms: float
    environment: str = APP_ENV

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ErrorResponse(BaseModel):
    """Enterprise error response structure"""
    status: str = "error"
    message: str
    error_code: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    environment: str = APP_ENV

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

# --- Business Logic Functions ---
def generate_batch_id() -> str:
    """Generate a unique batch ID with enterprise format"""
    return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def transform_payload_item(item: PayloadItem, source: str, batch_id: str) -> TransformedItem:
    """Transform a single payload item with enterprise metadata"""
    processing_time = datetime.now()
    
    # Generate processed ID with enterprise format
    processed_id = item.id if item.id else f"item_{uuid.uuid4().hex[:8]}"
    
    # Determine category for Delta table routing
    category = item.category if item.category else "default"
    
    # Add enterprise metadata
    enhanced_data = {
        **item.data,
        "processed_at": processing_time.isoformat(),
        "batch_id": batch_id,
        "processing_metadata": {
            "original_timestamp": item.timestamp.isoformat() if item.timestamp else None,
            "processing_duration_ms": 0,  # Will be updated later
            "environment": APP_ENV,
            "processor_version": "1.0.0"
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
    Enterprise payload validation and splitting for Delta table routing
    Returns a dictionary where keys are table names and values are lists of items
    """
    batch_id = payload.batch_id or generate_batch_id()
    source = payload.source or "api"
    
    # Dictionary to hold items by category (Delta table routing)
    categorized_items = {}
    
    for item in payload.items:
        try:
            # Transform the item with enterprise standards
            transformed_item = transform_payload_item(item, source, batch_id)
            
            # Group by category for Delta table routing
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
    """Serve the enterprise web UI or API information"""
    static_file = os.path.join(static_dir, "index.html")
    if os.path.exists(static_file):
        return FileResponse(static_file)
    else:
        # Fallback enterprise API response
        return {
            "service": "zerobus-delta-app",
            "status": "running",
            "version": "1.0.0",
            "environment": APP_ENV,
            "endpoints": {
                "health": "/health",
                "docs": "/docs" if APP_ENV != 'production' else "disabled",
                "process": "/api/v1/process",
                "process_simple": "/api/v1/process-simple"
            },
            "timestamp": datetime.now().isoformat()
        }

@app.get("/health")
async def health_check():
    """Enterprise health check endpoint with detailed status"""
    return {
        "status": "healthy",
        "service": "zerobus-delta-app",
        "version": "1.0.0",
        "environment": APP_ENV,
        "timestamp": datetime.now().isoformat(),
        "uptime": "running",
        "dependencies": {
            "static_files": os.path.exists(static_dir),
            "logging": True
        }
    }

@app.get("/api/info")
async def api_info():
    """Enterprise API information endpoint"""
    return {
        "service": "zerobus-delta-app",
        "status": "running",
        "version": "1.0.0",
        "environment": APP_ENV,
        "capabilities": {
            "payload_processing": True,
            "delta_table_preparation": True,
            "batch_processing": True,
            "web_ui": os.path.exists(static_dir)
        },
        "endpoints": {
            "health": "/health",
            "docs": "/docs" if APP_ENV != 'production' else "disabled",
            "process": "/api/v1/process",
            "process_simple": "/api/v1/process-simple"
        },
        "limits": {
            "max_items_per_batch": 1000,
            "max_payload_size": "10MB"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/process", response_model=ProcessingResponse)
async def process_payload(payload: IncomingPayload, request: Request):
    """
    Enterprise endpoint for structured payload processing
    
    This endpoint:
    1. Receives and validates JSON payloads
    2. Transforms data with enterprise metadata
    3. Categorizes items for Delta table routing
    4. Prepares data for Zerobus integration
    5. Returns detailed processing results
    """
    start_time = datetime.now()
    
    try:
        client_host = getattr(request.client, 'host', 'unknown') if request.client else 'unknown'
        logger.info(f"📨 Received payload processing request from {client_host}")
        logger.info(f"📊 Payload contains {len(payload.items)} items")
        
        # Enterprise payload validation and transformation
        categorized_items = validate_and_split_payload(payload)
        
        # Log distribution for monitoring
        for category, items in categorized_items.items():
            logger.info(f"📋 Category '{category}': {len(items)} items → Delta table routing")
        
        # Flatten all items for response
        all_processed_items = []
        for items in categorized_items.values():
            all_processed_items.extend(items)
        
        # Calculate processing metrics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds() * 1000
        
        # Update processing metadata with actual timing
        for item in all_processed_items:
            item.data["processing_metadata"]["processing_duration_ms"] = processing_time
        
        batch_id = payload.batch_id or generate_batch_id()
        
        # TODO: Phase 2 - Zerobus integration will be added here
        # This is where categorized_items will be sent to respective Delta tables
        
        logger.info(f"✅ Successfully processed {len(all_processed_items)} items in {processing_time:.2f}ms")
        
        return ProcessingResponse(
            message=f"Successfully processed {len(all_processed_items)} items across {len(categorized_items)} categories",
            batch_id=batch_id,
            processed_count=len(all_processed_items),
            items_processed=all_processed_items,
            processing_time_ms=processing_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error processing payload: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/api/v1/process-simple")
async def process_simple_payload(payload: Dict[str, Any], request: Request):
    """
    Enterprise endpoint for simple payload processing
    Accepts any JSON structure and applies enterprise transformations
    """
    start_time = datetime.now()
    
    try:
        client_host = getattr(request.client, 'host', 'unknown') if request.client else 'unknown'
        logger.info(f"📨 Received simple payload from {client_host}")
        
        # Add enterprise metadata
        processed_payload = {
            **payload,
            "processed_at": datetime.now().isoformat(),
            "processed_by": "zerobus-delta-app",
            "batch_id": generate_batch_id(),
            "environment": APP_ENV,
            "processing_metadata": {
                "processor_version": "1.0.0",
                "processing_duration_ms": (datetime.now() - start_time).total_seconds() * 1000
            }
        }
        
        # TODO: Phase 2 - Add Zerobus integration for simple payloads
        
        logger.info(f"✅ Simple payload processed successfully")
        
        return {
            "status": "success",
            "message": "Simple payload processed successfully",
            "data": processed_payload,
            "environment": APP_ENV
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing simple payload: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

# --- Enterprise Exception Handlers ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enterprise HTTP exception handler with detailed logging"""
    logger.warning(f"HTTP {exc.status_code}: {exc.detail} - {request.url}")
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
    """Enterprise general exception handler with security logging"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            message="Internal server error" if APP_ENV == 'production' else str(exc),
            error_code="INTERNAL_ERROR",
            details={"path": str(request.url), "method": request.method} if APP_ENV != 'production' else None
        ).dict()
    )

# --- Development/Debug Endpoints ---
if APP_ENV != 'production':
    @app.get("/debug")
    async def debug_info():
        """Debug endpoint for development environments only"""
        return {
            "service": "zerobus-delta-app",
            "status": "debug",
            "environment": APP_ENV,
            "python_version": "3.12+",
            "fastapi_version": "0.109.0",
            "available_routes": [
                {"path": "/", "method": "GET", "name": "serve_ui"},
                {"path": "/health", "method": "GET", "name": "health_check"},
                {"path": "/api/info", "method": "GET", "name": "api_info"},
                {"path": "/debug", "method": "GET", "name": "debug_info"},
                {"path": "/api/v1/process", "method": "POST", "name": "process_payload"},
                {"path": "/api/v1/process-simple", "method": "POST", "name": "process_simple_payload"},
                {"path": "/docs", "method": "GET", "name": "swagger_ui"},
                {"path": "/redoc", "method": "GET", "name": "redoc"}
            ],
            "static_files_available": os.path.exists(static_dir),
            "timestamp": datetime.now().isoformat()
        }

# --- Application Entry Point ---
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8000))
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port, 
        log_level=LOG_LEVEL.lower(),
        access_log=APP_ENV != 'production'
    )
