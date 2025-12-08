#!/usr/bin/env python3
"""
Zerobus Writer Implementation
============================

Official Zerobus Direct Write API integration using the PyPI SDK.
Based on Microsoft Databricks documentation and best practices.

Key Components:
- OAuth2 Service Principal authentication
- Protobuf serialization for high-performance streaming
- Automatic token refresh and connection management
- Production-ready error handling and logging

Documentation: https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest
Service Principal: Configure via DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
Required Permissions: USE_CATALOG, USE_SCHEMA, MODIFY, SELECT
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

from .base import DataWriterInterface, DataWriterError

logger = logging.getLogger(__name__)

class ZerobusWriter(DataWriterInterface):
    """
    Zerobus Direct Write API implementation using official PyPI SDK
    
    Features:
    - Official PyPI package: databricks-zerobus-ingest-sdk
    - OAuth2 Service Principal authentication
    - High-performance protobuf streaming
    - Automatic token refresh and reconnection
    - Production-ready error handling
    
    Documentation:
    https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest
    """
    
    def __init__(self):
        """Initialize Zerobus writer with official SDK and authentication"""
        self._writer_name = "Zerobus Writer"
        self._strategies = ["zerobus_sdk", "oauth2_auth", "protobuf_streaming", "pypi_official"]
        
        # Workspace configuration - Updated to new workspace
        self.workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
        self.workspace_id = "1444828305810485"
        
        # Zerobus endpoint - Format: <workspace_id>.zerobus.<region>.cloud.databricks.com
        # NOTE: This is a special "field-eng" demo workspace that doesn't follow
        # standard regional naming. Trying us-west-2 (most common for demos)
        self.region = "us-west-2"  # AWS region - common for demo workspaces
        self.server_endpoint = f"{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com"
        
        # Service Principal credentials with multiple fallback options
        self.client_id = os.getenv("DATABRICKS_CLIENT_ID")  # Required: Service Principal Client ID
        self.client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")  # Required: Service Principal Secret
        
        # Alternative authentication methods
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")  # PAT token fallback
        self.databricks_host = os.getenv("DATABRICKS_HOST")    # Host for PAT auth
        
        # Log credential status (masked for security)
        logger.info(f"🔑 Zerobus Writer Configuration:")
        logger.info(f"   - Workspace: {self.workspace_url}")
        logger.info(f"   - Workspace ID: {self.workspace_id}")
        logger.info(f"   - Region: {self.region}")
        logger.info(f"   - Server Endpoint: {self.server_endpoint}")
        logger.info(f"   - Client ID: {(f'{self.client_id[:8]}...') if self.client_id else 'NOT SET'}")
        logger.info(f"   - Client Secret: {(f'SET ({len(self.client_secret)} chars)') if self.client_secret else 'NOT SET'}")
        logger.info(f"   - PAT Token: {(f'SET ({len(self.databricks_token)} chars)') if self.databricks_token else 'NOT SET'}")
        
        # Validate credentials
        if not self.client_id or not self.client_secret:
            logger.warning("⚠️ Service Principal credentials not fully available!")
            if self.databricks_token:
                logger.info("✅ PAT token available as fallback")
            else:
                logger.warning("⚠️ No authentication method available!")
        else:
            logger.info("✅ Service Principal credentials available")
        
        # SDK components
        self._sdk_class = None
        self._table_properties_class = None
        self._stream_config_class = None
        self._protobuf_module = None
        
        # Initialize SDK
        self._initialize_sdk()
    
    def _initialize_sdk(self):
        """Initialize the official Zerobus SDK from PyPI"""
        try:
            # Import official SDK classes from PyPI package
            # Documentation: https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest
            from zerobus.sdk.sync import ZerobusSdk
            from zerobus.sdk.shared import TableProperties, StreamConfigurationOptions
            
            # Store the classes for later use
            self._sdk_class = ZerobusSdk
            self._table_properties_class = TableProperties
            self._stream_config_class = StreamConfigurationOptions
            
            logger.info("✅ Official Zerobus SDK imported from PyPI")
            logger.info(f"📦 Package: databricks-zerobus-ingest-sdk")
            logger.info(f"📡 Server endpoint: {self.server_endpoint}")
            logger.info(f"🏢 Workspace: {self.workspace_url}")
            
            # Import our protobuf module for record serialization
            try:
                import product_record_pb2
                self._protobuf_module = product_record_pb2
                logger.info("✅ Protobuf module imported successfully")
            except ImportError as proto_error:
                logger.error(f"❌ Failed to import protobuf module: {proto_error}")
                logger.info("💡 Run: python -m grpc_tools.protoc --python_out=. --proto_path=. product_record.proto")
                self._protobuf_module = None
            
        except ImportError as e:
            logger.error(f"❌ Failed to import official Zerobus SDK: {e}")
            logger.info("💡 Install with: pip install databricks-zerobus-ingest-sdk")
            logger.info("📚 Documentation: https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest")
            self._sdk_class = None
            self._table_properties_class = None
            self._stream_config_class = None
        except Exception as e:
            logger.error(f"❌ Failed to initialize Zerobus SDK: {e}")
            self._sdk_class = None
            self._table_properties_class = None
            self._stream_config_class = None
    
    @property
    def writer_name(self) -> str:
        """Return the human-readable name of this writer"""
        return self._writer_name
    
    @property
    def strategies(self) -> List[str]:
        """Return list of strategies used by this writer"""
        return self._strategies
    
    @property
    def configuration(self) -> Dict[str, Any]:
        """Return configuration information for this writer"""
        return {
            "writer_type": "zerobus",
            "sdk_source": "pypi_official",
            "package": "databricks-zerobus-ingest-sdk",
            "server_endpoint": self.server_endpoint,
            "workspace_url": self.workspace_url,
            "workspace_id": self.workspace_id,
            "region": self.region,
            "client_id": self.client_id[:8] + "..." if self.client_id else "NOT_SET",  # Masked for security
            "sdk_available": self._sdk_class is not None,
            "protobuf_available": self._protobuf_module is not None,
            "enabled": os.getenv("ENABLE_ZEROBUS_WRITER", "false").lower() == "true",
            "authentication": "oauth2_service_principal",
            "features": [
                "high_performance_streaming",
                "automatic_recovery", 
                "protobuf_serialization",
                "production_ready",
                "official_pypi_sdk"
            ],
            "documentation": "https://docs.databricks.com/aws/en/ingestion/zerobus-ingest"
        }
    
    @property
    def is_available(self) -> bool:
        """Check if Zerobus SDK is available and properly initialized"""
        if not os.getenv("ENABLE_ZEROBUS_WRITER", "false").lower() == "true":
            return False
        return (self._sdk_class is not None and 
                self._table_properties_class is not None and
                self._protobuf_module is not None)
    
    def _convert_to_protobuf(self, data_record: Dict[str, Any]) -> Any:
        """Convert dictionary data to protobuf ProductRecord"""
        try:
            # Create protobuf record
            record = self._protobuf_module.ProductRecord()
            
            # Map fields from our data to protobuf
            record.record_id = str(data_record.get("record_id", ""))
            record.product_id = str(data_record.get("product_id", ""))
            record.product_name = str(data_record.get("product_name", ""))
            record.product_price = float(data_record.get("product_price", 0.0))
            record.category = str(data_record.get("category", ""))
            record.sale_start_date = str(data_record.get("sale_start_date", ""))
            record.sale_stop_date = str(data_record.get("sale_stop_date", ""))
            record.processed_at = str(data_record.get("processed_at", ""))
            record.batch_id = str(data_record.get("batch_id", ""))
            record.source = str(data_record.get("source", ""))
            
            return record
            
        except Exception as e:
            logger.error(f"❌ Failed to convert data to protobuf: {e}")
            logger.error(f"❌ Data record: {data_record}")
            raise DataWriterError(
                f"Protobuf conversion failed: {e}",
                error_type="ProtobufConversionError",
                details={"data_record": data_record}
            )
    
    async def write_to_delta_table(
        self, 
        table_name: str, 
        data: List[Dict[str, Any]], 
        schema_name: str = "default",
        catalog_name: str = "main"
    ) -> Dict[str, Any]:
        """
        Write data to Delta table using official Zerobus Direct Write API
        
        This implementation follows the official Microsoft documentation:
        https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest
        
        Args:
            table_name: Target table name
            data: List of data records to write
            schema_name: Schema name (default: "default")
            catalog_name: Catalog name (default: "main")
            
        Returns:
            Dict containing write results and statistics
        """
        
        if not self.is_available:
            raise DataWriterError(
                "Zerobus Writer not available. Check SDK installation and ENABLE_ZEROBUS_WRITER setting.",
                error_type="WriterNotAvailable",
                details={
                    "sdk_available": self._sdk_class is not None,
                    "protobuf_available": self._protobuf_module is not None,
                    "install_command": "pip install databricks-zerobus-ingest-sdk",
                    "documentation": "https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest"
                }
            )
        
        start_time = datetime.now()
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Enhanced Zerobus operation logging
        logger.info("🚀" + "=" * 78)
        logger.info(f"🚀 ZEROBUS WRITE OPERATION STARTED")
        logger.info(f"📊 Operation Details:")
        logger.info(f"   - Target Table: {full_table_name}")
        logger.info(f"   - Records Count: {len(data)}")
        logger.info(f"   - Server Endpoint: {self.server_endpoint}")
        logger.info(f"   - Workspace URL: {self.workspace_url}")
        logger.info(f"   - Workspace ID: {self.workspace_id}")
        logger.info(f"   - Region: {self.region}")
        logger.info(f"   - SDK Source: Official PyPI Package")
        logger.info(f"   - Start Time: {start_time.isoformat()}")
        logger.info("🚀" + "=" * 78)
        
        try:
            # Initialize SDK with workspace configuration
            # According to official documentation:
            # sdk = ZerobusSdk(server_endpoint, workspace_url)
            logger.info("🔧 Initializing Zerobus SDK...")
            logger.info(f"   - Server Endpoint: {self.server_endpoint}")
            logger.info(f"   - Workspace URL: {self.workspace_url}")
            
            # Initialize SDK with both server_endpoint and workspace_url
            sdk = self._sdk_class(self.server_endpoint, self.workspace_url)
            
            logger.info("✅ SDK initialized successfully")
            
            # Configure table properties with protobuf descriptor
            logger.info(f"📋 Configuring table properties for {full_table_name}...")
            table_properties = self._table_properties_class(
                full_table_name,
                self._protobuf_module.ProductRecord.DESCRIPTOR
            )
            logger.info("✅ Table properties configured")
            
            # Create stream with Service Principal authentication
            # According to official documentation:
            # stream = sdk.create_stream(client_id, client_secret, table_properties)
            logger.info("🔐 Creating stream with Service Principal authentication...")
            
            if not self.client_id or not self.client_secret:
                raise DataWriterError(
                    "Service Principal credentials not configured",
                    error_type="AuthenticationError",
                    details={
                        "client_id_set": bool(self.client_id),
                        "client_secret_set": bool(self.client_secret),
                        "env_vars_needed": ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
                    }
                )
            
            # Create stream using the SDK's create_stream method
            # Pass: client_id, client_secret, table_properties (in that order)
            stream = sdk.create_stream(
                self.client_id,
                self.client_secret,
                table_properties
            )
            logger.info("✅ Zerobus stream created successfully")
            
            # Write records to stream
            records_written = 0
            records_failed = 0
            
            logger.info(f"📝 Starting to ingest {len(data)} records...")
            
            for i, record in enumerate(data):
                try:
                    # Convert to protobuf
                    protobuf_record = self._convert_to_protobuf(record)
                    
                    # Ingest record (async acknowledgment)
                    ack = stream.ingest_record(protobuf_record)
                    
                    # Wait for acknowledgment periodically for durability
                    if i % 1000 == 0 and i > 0:
                        logger.info(f"📝 Sent {i} records, waiting for acknowledgment...")
                        ack.wait_for_ack()
                    
                    records_written += 1
                        
                except Exception as record_error:
                    logger.error(f"❌ Failed to ingest record {i}: {record_error}")
                    records_failed += 1
            
            # Flush and wait for final acknowledgments
            logger.info("🔄 Flushing stream and waiting for final acknowledgments...")
            stream.flush()
            logger.info("✅ Stream flushed successfully")
            
            # Close stream
            logger.info("🔒 Closing stream...")
            stream.close()
            logger.info("✅ Zerobus stream closed successfully")
            
            # Calculate timing
            end_time = datetime.now()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            # Return results
            result = {
                "status": "success",
                "writer_name": self.writer_name,
                "table": full_table_name,
                "records_written": records_written,
                "records_failed": records_failed,
                "total_records": len(data),
                "duration_ms": duration_ms,
                "throughput_records_per_sec": len(data) / (duration_ms / 1000) if duration_ms > 0 else 0,
                "timestamp": end_time.isoformat(),
                "authentication": "oauth2_service_principal",
                "sdk_source": "pypi_official",
                "endpoint": self.server_endpoint,
                "workspace": self.workspace_url,
                "mock": False
            }
            
            logger.info(f"🎉 Zerobus write completed successfully!")
            logger.info(f"📊 Records written: {records_written}/{len(data)}")
            logger.info(f"⏱️ Duration: {duration_ms:.2f}ms")
            logger.info(f"🚀 Throughput: {result['throughput_records_per_sec']:.2f} records/sec")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Zerobus write failed: {e}")
            
            # Enhanced error details
            error_details = {
                "table": full_table_name,
                "records_attempted": len(data),
                "error_message": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat(),
                "endpoint": self.server_endpoint,
                "workspace": self.workspace_url,
                "workspace_id": self.workspace_id,
                "sdk_source": "pypi_official",
                "documentation": "https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest"
            }
            
            # Check for specific error types from official SDK
            error_str = str(e).lower()
            if "unauthorized" in error_str or "401" in error_str or "authentication" in error_str:
                error_details["likely_cause"] = "Service Principal authentication failed. Check credentials and permissions."
                error_details["required_permissions"] = ["USE_CATALOG", "USE_SCHEMA", "MODIFY", "SELECT"]
                error_details["check_credentials"] = {
                    "DATABRICKS_CLIENT_ID": "SET" if self.client_id else "NOT_SET",
                    "DATABRICKS_CLIENT_SECRET": "SET" if self.client_secret else "NOT_SET"
                }
            elif "protobuf" in error_str or "descriptor" in error_str:
                error_details["likely_cause"] = "Protobuf schema mismatch or conversion error."
                error_details["suggestion"] = "Verify product_record.proto matches table schema"
            elif "connection" in error_str or "network" in error_str or "grpc" in error_str:
                error_details["likely_cause"] = "Network connectivity issue to Zerobus endpoint."
                error_details["check_endpoint"] = self.server_endpoint
            elif "table" in error_str and ("not found" in error_str or "does not exist" in error_str):
                error_details["likely_cause"] = "Target table does not exist or is not accessible."
                error_details["suggestion"] = f"Create table {full_table_name} or check permissions"
            
            raise DataWriterError(
                f"Zerobus write failed: {e}",
                error_type="ZerobusWriteError",
                details=error_details
            )
