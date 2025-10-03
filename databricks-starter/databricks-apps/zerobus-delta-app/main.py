#!/usr/bin/env python3
"""
Databricks Delta App - Main Application Module

This is the main FastAPI application for the Databricks Delta App.
It provides web interface and REST API for processing structured data
and writing to Delta tables using a modular writer system.

This follows Databricks Apps best practices for:
- Modular code organization
- Proper naming conventions
- Environment-based configuration
- Comprehensive logging and monitoring

Key Features:
- Web UI for interactive data submission
- REST API endpoints for programmatic access
- Modular data writer system with pluggable implementations
- Comprehensive logging and error handling
- Support for multiple data schemas (Products, Users, Orders, Custom)
- Easy integration of future Zerobus SDK when ready

Current Data Writers:
- MockDataWriter: Always available fallback (currently active)
- DirectDeltaWriter: Direct SQL via Databricks SDK (disabled by default)
- ZerobusWriter: Future Zerobus SDK integration (placeholder)

Author: Assistant
Created: 2025-10-02
Updated: 2025-10-03 - Applied Databricks Apps naming conventions
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid
import os

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Store logs in memory for debugging
import io
log_stream = io.StringIO()
log_handler = logging.StreamHandler(log_stream)
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(log_handler)

# Initialize FastAPI app
app = FastAPI(
    title="Databricks Direct Write App",
    description="FastAPI app for processing structured data and writing directly to Delta tables",
    version="2.0.0"
)

# Mount static files for web UI
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================================
# PYDANTIC MODELS
# ================================

class ProductItem(BaseModel):
    """Product data model matching Delta table schema"""
    product_id: str = Field(..., description="Unique product identifier")
    product_name: str = Field(..., description="Product name")
    product_price: float = Field(..., ge=0, description="Product price (must be >= 0)")
    category: str = Field(..., description="Product category")
    sale_start_date: str = Field(..., description="Sale start date (YYYY-MM-DD)")
    sale_stop_date: str = Field(..., description="Sale stop date (YYYY-MM-DD)")

class StructuredPayload(BaseModel):
    """Structured payload containing multiple items"""
    schema_type: str = Field(default="products", description="Data schema type")
    items: List[ProductItem] = Field(..., description="List of product items")
    writer_type: Optional[str] = Field(default="auto", description="Preferred writer type (direct_delta, zerobus, mock, auto)")

class ProcessingResponse(BaseModel):
    """Response model for processing endpoints"""
    message: str
    batch_id: str
    processed_count: int
    processing_time_ms: float
    zerobus_integration: Dict[str, Any]
    status: str
    processed_data: List[Dict[str, Any]]

def create_writer_by_type(writer_type: str):
    """
    Create a data writer based on the specified type
    
    Args:
        writer_type: Type of writer to create ('direct_delta', 'zerobus', 'mock', 'auto')
        
    Returns:
        DataWriterInterface: The requested writer instance
    """
    
    logger.info(f"🔧 Creating writer of type: {writer_type}")
    
    if writer_type == "direct_delta":
        # Force Direct Delta Writer
        try:
            from writers.direct_delta import DirectDeltaWriter
            writer = DirectDeltaWriter()
            if writer.is_available:
                logger.info("✅ Created Direct Delta Writer (user requested)")
                return writer
            else:
                logger.warning("⚠️ Direct Delta Writer requested but not available, falling back to mock")
                from writers.base import MockDataWriter
                return MockDataWriter()
        except ImportError as e:
            logger.error(f"❌ Failed to import Direct Delta Writer: {e}")
            from writers.base import MockDataWriter
            return MockDataWriter()
    
    elif writer_type == "zerobus":
        # Force Zerobus Writer
        try:
            from writers.zerobus import ZerobusWriter
            writer = ZerobusWriter()
            if writer.is_available:
                logger.info("✅ Created Zerobus Writer (user requested)")
                return writer
            else:
                logger.warning("⚠️ Zerobus Writer requested but not available, falling back to mock")
                from writers.base import MockDataWriter
                return MockDataWriter()
        except ImportError as e:
            logger.error(f"❌ Failed to import Zerobus Writer: {e}")
            from writers.base import MockDataWriter
            return MockDataWriter()
    
    elif writer_type == "mock":
        # Force Mock Writer
        logger.info("🧪 Created Mock Writer (user requested)")
        from writers.base import MockDataWriter
        return MockDataWriter()
    
    else:
        # Auto selection (default factory behavior)
        logger.info("🔄 Using automatic writer selection")
        from writers import create_writer
        return create_writer()

# ================================
# UTILITY FUNCTIONS
# ================================

def process_product_data(items: List[ProductItem], batch_id: str) -> List[Dict[str, Any]]:
    """
    Process product items and add metadata fields
    
    Args:
        items: List of ProductItem objects
        batch_id: Unique batch identifier
        
    Returns:
        List of processed data dictionaries
    """
    processed_data = []
    
    for item in items:
        # Convert Pydantic model to dict
        item_dict = item.dict()
        
        # Add processing metadata
        item_dict.update({
            "record_id": str(uuid.uuid4()),
            "processed_at": datetime.now().isoformat(),
            "batch_id": batch_id,
            "source": "structured_payload"
        })
        
        processed_data.append(item_dict)
        logger.info(f"Processed item: {item_dict['product_id']}")
    
    return processed_data

# ================================
# API ENDPOINTS
# ================================

@app.get("/")
async def serve_ui():
    """Serve the main web UI"""
    return FileResponse("static/index.html")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "app": "databricks-direct-write-app",
        "version": "2.0.0"
    }

@app.post("/api/v1/process-structured", response_model=ProcessingResponse)
async def process_structured_payload(payload: StructuredPayload):
    """
    Process structured payload and write to Delta table
    
    This endpoint:
    1. Validates the incoming structured data
    2. Adds processing metadata (IDs, timestamps, batch info)
    3. Writes data to Delta table via direct SQL approach
    4. Returns processing results and status
    """
    start_time = datetime.now()
    batch_id = str(uuid.uuid4())
    
    logger.info(f"Processing structured payload with {len(payload.items)} items")
    
    try:
        # Phase 1: Data Processing
        processed_data = process_product_data(payload.items, batch_id)
        logger.info(f"✅ Processed {len(processed_data)} items successfully")
        
        # Phase 2: Data Writing (Modular System)
        logger.info("🔄 Starting data writing...")
        
        try:
            # Create the user-requested data writer
            data_writer = create_writer_by_type(payload.writer_type)
            logger.info(f"✅ Data writer created: {data_writer.writer_name}")
            logger.info(f"✅ Writer strategies: {data_writer.strategies}")
            logger.info(f"✅ Writer available: {data_writer.is_available}")
            logger.info(f"👤 User requested: {payload.writer_type}")
            
            # Write to Delta table
            table_name = f"delta_{payload.schema_type}_data"
            logger.info(f"📝 Writing {len(processed_data)} records to table: {table_name}")
            
            write_result = await data_writer.write_to_delta_table(
                table_name=table_name,
                data=processed_data,
                schema_name="delta_app",
                catalog_name="kaustavpaul_demo"
            )
            
            logger.info(f"📊 Data write result: {write_result}")
            
        except ImportError as e:
            logger.error(f"❌ Failed to import writer: {e}")
            write_result = {
                "status": "failed",
                "error": f"Writer not available: {e}",
                "error_type": "ImportError",
                "mock": True,
                "reason": "Module import failed",
                "user_requested": payload.writer_type
            }
        except Exception as e:
            # Import DataWriterError for proper exception handling
            try:
                from writers.base import DataWriterError
                if isinstance(e, DataWriterError):
                    logger.error(f"❌ Data writer error: {e}")
                    write_result = {
                        "status": "failed",
                        "error": str(e),
                        "error_type": e.error_type,
                        "details": e.details,
                        "mock": False,
                        "reason": "Data writer failed",
                        "user_requested": payload.writer_type
                    }
                else:
                    raise e  # Re-raise if not DataWriterError
            except ImportError:
                # DataWriterError not available, treat as generic exception
                logger.error(f"❌ Unexpected data write error: {e}")
                import traceback
                logger.error(f"❌ Full traceback: {traceback.format_exc()}")
                write_result = {
                    "status": "failed",
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "mock": False,
                    "reason": "Unexpected error",
                    "user_requested": payload.writer_type,
                    "traceback": traceback.format_exc()
                }
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return ProcessingResponse(
            message=f"Successfully processed {len(payload.items)} items",
            batch_id=batch_id,
            processed_count=len(processed_data),
            processing_time_ms=processing_time,
            zerobus_integration=write_result,
            status="success",
            processed_data=processed_data
        )
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "batch_id": batch_id,
                "processing_time_ms": processing_time
            }
        )

# ================================
# DEBUG ENDPOINTS
# ================================

@app.get("/debug/logs")
async def get_logs():
    """Get recent application logs for debugging"""
    logs = log_stream.getvalue().split('\n')
    recent_logs = [log for log in logs[-100:] if log.strip()]  # Last 100 non-empty lines
    
    return {
        "timestamp": datetime.now().isoformat(),
        "total_lines": len(recent_logs),
        "logs": recent_logs,
        "note": "Showing last 100 log lines"
    }

@app.get("/debug/environment")
async def get_environment():
    """Get environment information for debugging"""
    return {
        "python_version": f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
        "environment_variables": {
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "Not set"),
            "DATABRICKS_TOKEN": "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT SET",
            "DATABRICKS_CLIENT_ID": "SET" if os.getenv("DATABRICKS_CLIENT_ID") else "NOT SET",
            "DATABRICKS_CLIENT_SECRET": "SET" if os.getenv("DATABRICKS_CLIENT_SECRET") else "NOT SET"
        },
        "working_directory": os.getcwd(),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/debug/writers")
async def check_writers_status():
    """Check all data writers availability and configuration"""
    try:
        from writers import get_writer_status
        return get_writer_status()
    except ImportError as e:
        return {
            "error": f"Writer factory not available: {e}",
            "timestamp": datetime.now().isoformat()
        }

# ================================
# APPLICATION STARTUP
# ================================

@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    logger.info("🚀 Databricks Direct Write App starting up...")
    logger.info("✅ FastAPI application initialized")
    logger.info("✅ Static files mounted")
    logger.info("✅ API endpoints registered")
    logger.info("🎯 App ready to process structured data!")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event"""
    logger.info("🛑 Databricks Direct Write App shutting down...")
    logger.info("✅ Cleanup completed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
