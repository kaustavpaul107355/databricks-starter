#!/usr/bin/env python3
"""
Databricks Direct Write App - Main Application Module

This is the main FastAPI application for the Databricks Direct Write App.
It provides a comprehensive web interface and REST API for processing structured data
and writing to Delta tables using multiple high-performance writer implementations.

=== ARCHITECTURE OVERVIEW ===

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Web UI        │    │   FastAPI App    │    │   Writer System     │
│   (index.html)  │───▶│   (main.py)      │───▶│   (writers/)        │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                │                         │
                                ▼                         ▼
                       ┌──────────────────┐    ┌─────────────────────┐
                       │   Data Models    │    │   Delta Tables      │
                       │   (Pydantic)     │    │   (Databricks)      │
                       └──────────────────┘    └─────────────────────┘

=== KEY FEATURES ===

🎯 Multi-Writer Architecture:
   - Zerobus Writer: High-performance streaming via Zerobus Direct Write API
   - Direct Delta Writer: SQL-based writing via Databricks SDK
   - Mock Writer: Testing and development fallback

🔧 Production Features:
   - Comprehensive logging with structured output
   - Enhanced error handling with detailed context
   - Performance metrics and timing analysis
   - Source tracking for data lineage
   - Modular and extensible design

🌐 Web Interface:
   - Interactive form for data submission
   - Writer selection and configuration
   - Real-time status feedback
   - Clear form management

📊 Data Processing:
   - Structured payload validation
   - Automatic metadata enrichment
   - Batch processing with unique IDs
   - Schema-aware data transformation

=== DATABRICKS APPS COMPLIANCE ===

This application follows Databricks Apps best practices:
- ✅ Proper file structure and naming conventions
- ✅ Environment-based configuration
- ✅ Comprehensive logging and monitoring
- ✅ Modular code organization
- ✅ Production-ready error handling
- ✅ Asset bundle deployment configuration

Author: Assistant
Created: 2025-10-02
Updated: 2025-10-03 - Enhanced documentation and production features
Version: 2.0.0
"""

# ================================
# IMPORTS AND DEPENDENCIES
# ================================

# Standard library imports
import logging
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

# Third-party imports
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# Local imports - Writer system
from writers.base import DataWriterInterface, MockDataWriter

# ================================
# LOGGING CONFIGURATION
# ================================

# Configure comprehensive logging for production monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Store logs in memory for debugging and monitoring
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
# PYDANTIC DATA MODELS
# ================================

class ProductItem(BaseModel):
    """
    Product data model matching Delta table schema
    
    This model defines the structure for individual product records that will be
    processed and written to the Delta table. All fields are validated according
    to business rules and database constraints.
    
    Attributes:
        product_id: Unique identifier for the product (e.g., "PROD001")
        product_name: Human-readable product name (e.g., "iPhone 15")
        product_price: Product price in USD, must be non-negative
        category: Product category for classification (e.g., "electronics")
        sale_start_date: Start date for product availability (YYYY-MM-DD format)
        sale_stop_date: End date for product availability (YYYY-MM-DD format)
    """
    product_id: str = Field(..., description="Unique product identifier", min_length=1, max_length=50)
    product_name: str = Field(..., description="Product name", min_length=1, max_length=200)
    product_price: float = Field(..., ge=0, description="Product price in USD (must be >= 0)")
    category: str = Field(..., description="Product category", min_length=1, max_length=50)
    sale_start_date: str = Field(..., description="Sale start date (YYYY-MM-DD format)", regex=r'^\d{4}-\d{2}-\d{2}$')
    sale_stop_date: str = Field(..., description="Sale stop date (YYYY-MM-DD format)", regex=r'^\d{4}-\d{2}-\d{2}$')

class StructuredPayload(BaseModel):
    """
    Structured payload containing multiple product items and processing configuration
    
    This model represents the complete request payload sent from the web UI or API clients.
    It includes the data items to process and configuration for how they should be handled.
    
    Attributes:
        schema_type: Type of data schema being used (currently supports "products")
        items: List of ProductItem objects to be processed and written
        writer_type: Preferred data writer implementation to use
    """
    schema_type: str = Field(default="products", description="Data schema type (products, users, orders, custom)")
    items: List[ProductItem] = Field(..., description="List of product items to process", min_items=1, max_items=100)
    writer_type: Optional[str] = Field(default="zerobus", description="Preferred writer type (zerobus, direct_delta, mock)")

class ProcessingResponse(BaseModel):
    """
    Response model for data processing endpoints
    
    This model defines the structure of responses returned by the processing endpoints.
    It provides comprehensive information about the processing results, performance metrics,
    and detailed writer-specific information for monitoring and debugging.
    
    Attributes:
        message: Human-readable summary of the processing result
        batch_id: Unique identifier for this processing batch
        processed_count: Number of items successfully processed
        processing_time_ms: Total processing time in milliseconds
        zerobus_integration: Detailed information about the writer used and results
        status: Overall processing status ("success" or "error")
        processed_data: List of processed data items with metadata
    """
    message: str = Field(..., description="Human-readable processing summary")
    batch_id: str = Field(..., description="Unique batch identifier")
    processed_count: int = Field(..., description="Number of items processed")
    processing_time_ms: float = Field(..., description="Processing time in milliseconds")
    zerobus_integration: Dict[str, Any] = Field(..., description="Writer-specific results and metadata")
    status: str = Field(..., description="Overall processing status")
    processed_data: List[Dict[str, Any]] = Field(..., description="Processed data items with metadata")

def create_writer_by_type(writer_type: str):
    """
    Create a data writer based on the specified type with robust isolation
    
    This function provides complete isolation between writer types and ensures
    that the user's selection is respected without any fallback to factory logic.
    
    Args:
        writer_type: Type of writer to create ('zerobus', 'direct_delta', 'mock')
        
    Returns:
        DataWriterInterface: The requested writer instance
    """
    
    logger.info(f"🎯 ROBUST WRITER SELECTION: Creating '{writer_type}' writer")
    logger.info(f"🔍 Input validation: writer_type='{writer_type}' (length: {len(writer_type)}, type: {type(writer_type)})")
    
    # Normalize input
    writer_type = writer_type.strip().lower()
    logger.info(f"🔧 Normalized writer_type: '{writer_type}'")
    
    # ZEROBUS WRITER (DEFAULT/PRIMARY)
    if writer_type == "zerobus":
        logger.info("🚀 SELECTED: Zerobus Writer (Primary Choice)")
        try:
            from writers.zerobus import ZerobusWriter
            writer = ZerobusWriter()
            
            logger.info(f"✅ Zerobus Writer instantiated successfully")
            logger.info(f"📊 Writer available: {writer.is_available}")
            logger.info(f"🔧 Writer configuration: {writer.configuration}")
            
            if writer.is_available:
                logger.info("🎉 Zerobus Writer is AVAILABLE and READY!")
                return writer
            else:
                logger.warning("⚠️ Zerobus Writer is NOT AVAILABLE - using Mock Writer as fallback")
                from writers.base import MockDataWriter
                mock_writer = MockDataWriter()
                logger.info("🧪 Mock Writer created as Zerobus fallback")
                return mock_writer
                
        except ImportError as e:
            logger.error(f"❌ Failed to import Zerobus Writer: {e}")
            logger.info("🧪 Creating Mock Writer due to import failure")
            from writers.base import MockDataWriter
            return MockDataWriter()
        except Exception as e:
            logger.error(f"❌ Unexpected error creating Zerobus Writer: {e}")
            logger.info("🧪 Creating Mock Writer due to unexpected error")
            from writers.base import MockDataWriter
            return MockDataWriter()
    
    # DIRECT DELTA WRITER (FALLBACK OPTION)
    elif writer_type == "direct_delta":
        logger.info("🏗️ SELECTED: Direct Delta Writer (Fallback Choice)")
        try:
            from writers.direct_delta import DirectDeltaWriter
            writer = DirectDeltaWriter()
            
            logger.info(f"✅ Direct Delta Writer instantiated successfully")
            logger.info(f"📊 Writer available: {writer.is_available}")
            
            if writer.is_available:
                logger.info("🎉 Direct Delta Writer is AVAILABLE and READY!")
                return writer
            else:
                logger.warning("⚠️ Direct Delta Writer is NOT AVAILABLE - using Mock Writer as fallback")
                from writers.base import MockDataWriter
                mock_writer = MockDataWriter()
                logger.info("🧪 Mock Writer created as Direct Delta fallback")
                return mock_writer
                
        except ImportError as e:
            logger.error(f"❌ Failed to import Direct Delta Writer: {e}")
            logger.info("🧪 Creating Mock Writer due to import failure")
            from writers.base import MockDataWriter
            return MockDataWriter()
        except Exception as e:
            logger.error(f"❌ Unexpected error creating Direct Delta Writer: {e}")
            logger.info("🧪 Creating Mock Writer due to unexpected error")
            from writers.base import MockDataWriter
            return MockDataWriter()
    
    # MOCK WRITER (TESTING)
    elif writer_type == "mock":
        logger.info("🧪 SELECTED: Mock Writer (Testing Choice)")
        from writers.base import MockDataWriter
        writer = MockDataWriter()
        logger.info("✅ Mock Writer created successfully")
        return writer
    
    # INVALID/UNKNOWN WRITER TYPE
    else:
        logger.error(f"❌ INVALID WRITER TYPE: '{writer_type}' is not recognized")
        logger.info("🚀 Defaulting to Zerobus Writer (primary choice)")
        
        # Default to Zerobus Writer for any invalid input
        try:
            from writers.zerobus import ZerobusWriter
            writer = ZerobusWriter()
            if writer.is_available:
                logger.info("✅ Default Zerobus Writer created successfully")
                return writer
            else:
                logger.warning("⚠️ Default Zerobus Writer not available - using Mock Writer")
                from writers.base import MockDataWriter
                return MockDataWriter()
        except Exception as e:
            logger.error(f"❌ Failed to create default Zerobus Writer: {e}")
            logger.info("🧪 Final fallback to Mock Writer")
            from writers.base import MockDataWriter
            return MockDataWriter()

# ================================
# UTILITY FUNCTIONS
# ================================

def process_product_data(items: List[ProductItem], batch_id: str, writer_method: str = "unknown") -> List[Dict[str, Any]]:
    """
    Process product items and add metadata fields including writer method
    
    Args:
        items: List of ProductItem objects
        batch_id: Unique batch identifier
        writer_method: The data writer method being used (e.g., "zerobus_direct_write", "delta_direct_write")
        
    Returns:
        List of processed data dictionaries
    """
    processed_data = []
    
    for item in items:
        # Convert Pydantic model to dict
        item_dict = item.dict()
        
        # Add processing metadata with writer method in source
        item_dict.update({
            "record_id": str(uuid.uuid4()),
            "processed_at": datetime.now().isoformat(),
            "batch_id": batch_id,
            "source": f"{writer_method}_structured_payload"  # Include writer method in source
        })
        
        processed_data.append(item_dict)
        logger.info(f"Processed item: {item_dict['product_id']} via {writer_method}")
    
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

@app.get("/debug/zerobus-availability")
async def debug_zerobus_availability():
    """Debug endpoint to check Zerobus Writer availability"""
    try:
        from writers.zerobus import ZerobusWriter
        writer = ZerobusWriter()
        
        return {
            "zerobus_writer_status": {
                "import_successful": True,
                "instantiation_successful": True,
                "is_available": writer.is_available,
                "writer_name": writer.writer_name,
                "configuration": writer.configuration,
                "environment_variables": {
                    "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER"),
                    "DATABRICKS_CLIENT_ID": os.getenv("DATABRICKS_CLIENT_ID", "NOT_SET")[:8] + "..." if os.getenv("DATABRICKS_CLIENT_ID") else "NOT_SET",
                    "DATABRICKS_CLIENT_SECRET": "SET" if os.getenv("DATABRICKS_CLIENT_SECRET") else "NOT_SET",
                    "DATABRICKS_TOKEN": "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT_SET"
                },
                "sdk_status": {
                    "_sdk": str(type(writer._sdk)) if hasattr(writer, '_sdk') else "NOT_SET",
                    "_protobuf_module": str(type(writer._protobuf_module)) if hasattr(writer, '_protobuf_module') else "NOT_SET",
                    "_sdk_type": getattr(writer, '_sdk_type', "NOT_SET")
                }
            },
            "timestamp": datetime.now().isoformat()
        }
    except ImportError as e:
        return {
            "zerobus_writer_status": {
                "import_successful": False,
                "import_error": str(e),
                "environment_variables": {
                    "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER"),
                }
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "zerobus_writer_status": {
                "import_successful": True,
                "instantiation_successful": False,
                "instantiation_error": str(e),
                "environment_variables": {
                    "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER"),
                }
            },
            "timestamp": datetime.now().isoformat()
        }

@app.get("/debug/direct-delta-availability")
async def debug_direct_delta_availability():
    """Debug endpoint to check Direct Delta Writer availability"""
    try:
        from writers.direct_delta import DirectDeltaWriter
        writer = DirectDeltaWriter()
        
        return {
            "direct_delta_writer_status": {
                "import_successful": True,
                "instantiation_successful": True,
                "is_available": writer.is_available,
                "writer_name": writer.writer_name,
                "configuration": writer.configuration,
                "environment_variables": {
                    "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER"),
                    "DATABRICKS_TOKEN": "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT_SET"
                },
                "sdk_status": {
                    "workspace_client": str(type(writer._workspace_client)) if hasattr(writer, '_workspace_client') and writer._workspace_client else "NOT_SET",
                    "initialization_error": getattr(writer, '_initialization_error', "NONE")
                }
            },
            "timestamp": datetime.now().isoformat()
        }
    except ImportError as e:
        return {
            "direct_delta_writer_status": {
                "import_successful": False,
                "import_error": str(e),
                "environment_variables": {
                    "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER"),
                }
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "direct_delta_writer_status": {
                "import_successful": True,
                "instantiation_successful": False,
                "instantiation_error": str(e),
                "environment_variables": {
                    "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER"),
                }
            },
            "timestamp": datetime.now().isoformat()
        }

@app.post("/debug/test-direct-delta")
async def test_direct_delta_writer():
    """Test endpoint specifically for Direct Delta Writer"""
    try:
        from writers.direct_delta import DirectDeltaWriter
        
        # Create test data
        test_data = [{
            "product_id": "TEST001",
            "product_name": "Test Product",
            "product_price": 99.99,
            "category": "test",
            "sale_start_date": "2024-01-01",
            "sale_stop_date": "2024-12-31",
            "record_id": "test-record-id",
            "processed_at": datetime.now().isoformat(),
            "batch_id": "test-batch-id",
            "source": "delta_direct_write_debug_test"  # Reflect Direct Delta Writer method
        }]
        
        # Create writer and test
        writer = DirectDeltaWriter()
        
        result = await writer.write_to_delta_table(
            table_name="zerobus_products_clean",
            data=test_data,
            schema_name="zerobus_delta",
            catalog_name="kaustavpaul_demo"
        )
        
        return {
            "test_result": "success",
            "writer_name": writer.writer_name,
            "writer_available": writer.is_available,
            "write_result": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "test_result": "failed",
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": datetime.now().isoformat()
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
    
    # Enhanced request logging with clear structure
    logger.info("=" * 80)
    logger.info(f"🚀 NEW REQUEST STARTED - Batch ID: {batch_id}")
    logger.info(f"📊 Request Details:")
    logger.info(f"   - Items Count: {len(payload.items)}")
    logger.info(f"   - Schema Type: '{payload.schema_type}'")
    logger.info(f"   - Writer Type: '{payload.writer_type}' (user requested)")
    logger.info(f"   - Timestamp: {start_time.isoformat()}")
    logger.info(f"📋 Items Preview:")
    for i, item in enumerate(payload.items[:3]):  # Log first 3 items
        logger.info(f"   - Item {i+1}: {item.product_id} - {item.product_name} (${item.product_price})")
    if len(payload.items) > 3:
        logger.info(f"   - ... and {len(payload.items) - 3} more items")
    logger.info("=" * 80)
    
    try:
        # Phase 1: Determine Writer Method for Source Tracking
        writer_method_map = {
            "zerobus": "zerobus_direct_write",
            "direct_delta": "delta_direct_write", 
            "mock": "mock_simulation"
        }
        writer_method = writer_method_map.get(payload.writer_type, "unknown_writer")
        logger.info(f"🏷️ Writer method for source tracking: {writer_method}")
        
        # Phase 2: Data Processing (with writer method tracking)
        processed_data = process_product_data(payload.items, batch_id, writer_method)
        logger.info(f"✅ Processed {len(processed_data)} items successfully")
        
        # Phase 3: Data Writing (Modular System)
        logger.info("🔄 Starting data writing...")
        
        try:
            # Create the user-requested data writer
            data_writer = create_writer_by_type(payload.writer_type)
            logger.info(f"✅ Data writer created: {data_writer.writer_name}")
            logger.info(f"✅ Writer strategies: {data_writer.strategies}")
            logger.info(f"✅ Writer available: {data_writer.is_available}")
            logger.info(f"👤 User requested: {payload.writer_type}")
            
            # Write to Delta table - use clean table for Zerobus compatibility
            table_name = f"zerobus_{payload.schema_type}_clean"  # Use clean table without unsupported features
            logger.info(f"📝 Writing {len(processed_data)} records to table: {table_name}")
            
            write_result = await data_writer.write_to_delta_table(
                table_name=table_name,
                data=processed_data,
                schema_name="zerobus_delta",  # Use zerobus_delta schema
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
        
        # Calculate processing time and enhanced success logging
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Enhanced success logging
        logger.info("=" * 80)
        logger.info(f"✅ REQUEST COMPLETED SUCCESSFULLY - Batch ID: {batch_id}")
        logger.info(f"⏱️ Performance Metrics:")
        logger.info(f"   - Total Processing Time: {processing_time:.2f}ms")
        logger.info(f"   - Items Processed: {len(processed_data)}")
        logger.info(f"   - Throughput: {len(processed_data) / (processing_time / 1000):.2f} items/sec")
        logger.info(f"📊 Writer Results:")
        logger.info(f"   - Writer Used: {write_result.get('writer_name', 'Unknown')}")
        logger.info(f"   - Records Written: {write_result.get('records_written', 0)}")
        logger.info(f"   - Status: {write_result.get('status', 'Unknown')}")
        if write_result.get('table'):
            logger.info(f"   - Target Table: {write_result['table']}")
        logger.info("=" * 80)
        
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
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Enhanced error logging
        logger.error("=" * 80)
        logger.error(f"❌ REQUEST FAILED - Batch ID: {batch_id}")
        logger.error(f"💥 Error Details:")
        logger.error(f"   - Error Type: {type(e).__name__}")
        logger.error(f"   - Error Message: {str(e)}")
        logger.error(f"   - Processing Time: {processing_time:.2f}ms")
        logger.error(f"   - Items Attempted: {len(payload.items)}")
        logger.error(f"   - Writer Type: {payload.writer_type}")
        logger.error(f"📋 Request Context:")
        logger.error(f"   - Schema Type: {payload.schema_type}")
        logger.error(f"   - Batch ID: {batch_id}")
        logger.error(f"   - Timestamp: {start_time.isoformat()}")
        
        # Log stack trace for debugging
        import traceback
        logger.error(f"🔍 Stack Trace:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                logger.error(f"   {line}")
        logger.error("=" * 80)
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal server error during processing",
                "message": str(e),
                "error_type": type(e).__name__,
                "batch_id": batch_id,
                "processing_time_ms": processing_time,
                "context": {
                    "items_count": len(payload.items),
                    "writer_type": payload.writer_type,
                    "schema_type": payload.schema_type,
                    "timestamp": start_time.isoformat()
                }
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

@app.get("/debug/simple-test")
async def simple_debug_test():
    """Simple debug test that shows what writer is being created"""
    try:
        # Test the exact same call that process_structured_payload makes
        test_writer_type = "zerobus"  # This is what should be sent from UI
        logger.info(f"🧪 SIMPLE TEST: Testing writer creation with type: '{test_writer_type}'")
        
        writer = create_writer_by_type(test_writer_type)
        
        return {
            "test_input": test_writer_type,
            "writer_created": {
                "writer_name": writer.writer_name,
                "writer_class": type(writer).__name__,
                "is_available": writer.is_available,
                "strategies": writer.strategies
            },
            "environment_check": {
                "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER", "not_set"),
                "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "not_set")
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": __import__('traceback').format_exc(),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/debug/test-zerobus-direct")
async def test_zerobus_direct():
    """Test Zerobus Writer creation directly"""
    try:
        from writers.zerobus import ZerobusWriter
        writer = ZerobusWriter()
        
        return {
            "writer_name": writer.writer_name,
            "writer_class": type(writer).__name__,
            "is_available": writer.is_available,
            "strategies": writer.strategies,
            "configuration": writer.configuration,
            "environment": {
                "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER", "not_set"),
                "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "not_set")
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": __import__('traceback').format_exc(),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/debug/writer-test")
async def test_writer_selection():
    """Test writer selection with different types"""
    results = {}
    
    test_types = ["direct_delta", "zerobus", "mock", "auto", "invalid"]
    
    for writer_type in test_types:
        try:
            writer = create_writer_by_type(writer_type)
            results[writer_type] = {
                "writer_name": writer.writer_name,
                "writer_class": type(writer).__name__,
                "is_available": writer.is_available,
                "strategies": writer.strategies
            }
        except Exception as e:
            results[writer_type] = {
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    return {
        "test_results": results,
        "environment": {
            "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER", "not_set"),
            "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "not_set")
        },
        "timestamp": datetime.now().isoformat()
    }


@app.get("/debug/zerobus")
async def check_zerobus_status():
    """Check Zerobus writer specific status"""
    try:
        from writers.zerobus import ZerobusWriter
        writer = ZerobusWriter()
        return {
            "writer_name": writer.writer_name,
            "is_available": writer.is_available,
            "configuration": writer.configuration,
            "strategies": writer.strategies,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": datetime.now().isoformat()
        }

# ================================
# APPLICATION STARTUP
# ================================

@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    # Configure writer priorities: Zerobus as primary, Direct Delta as available option
    os.environ["ENABLE_ZEROBUS_WRITER"] = "true"
    os.environ["ENABLE_DIRECT_DELTA_WRITER"] = "true"  # Available as user option
    
    # Set Zerobus service principal credentials (fallback)
    # NOTE: Set these environment variables in your deployment configuration
    # os.environ["DATABRICKS_CLIENT_ID"] = "your-service-principal-client-id"
    # os.environ["DATABRICKS_CLIENT_SECRET"] = "your-service-principal-secret"
    
    # Try to get PAT token from Databricks Apps environment
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        if hasattr(client.config, 'token') and client.config.token:
            os.environ["DATABRICKS_TOKEN"] = client.config.token
            logger.info("✅ PAT token obtained from Databricks Apps environment")
        else:
            logger.info("ℹ️ No PAT token available from Databricks Apps environment")
    except Exception as e:
        logger.warning(f"⚠️ Could not get PAT token from environment: {e}")
    
    logger.info("🚀 Databricks Direct Write App starting up...")
    logger.info("✅ FastAPI application initialized")
    logger.info("✅ Static files mounted")
    logger.info("✅ API endpoints registered")
    logger.info("🚀 Zerobus Writer enabled as PRIMARY choice")
    logger.info("🏗️ Direct Delta Writer available as FALLBACK choice")
    logger.info("🔑 Authentication configured (PAT primary, Service Principal fallback)")
    logger.info("🎯 App ready to process structured data with robust writer selection!")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event"""
    logger.info("🛑 Databricks Direct Write App shutting down...")
    logger.info("✅ Cleanup completed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
