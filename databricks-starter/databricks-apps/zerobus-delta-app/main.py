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
    writer_type: Optional[str] = Field(default="zerobus", description="Preferred writer type (zerobus, direct_delta, mock)")

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
    os.environ["DATABRICKS_CLIENT_ID"] = "e2037d44-6c92-4fee-9ed5-e59f70eb7107"  # gitleaks:allow
    os.environ["DATABRICKS_CLIENT_SECRET"] = "dose127056941651a9e3019408598d394cce"  # gitleaks:allow
    
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
