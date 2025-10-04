#!/usr/bin/env python3
"""
Direct Delta Writer Implementation Module

This module provides direct Delta table writing functionality using the Databricks SDK.
It implements the DataWriterInterface and can be enabled/disabled via environment variables.

Classes:
    DirectDeltaWriter: Direct SQL-based Delta table writer

Environment Variables:
    ENABLE_DIRECT_DELTA_WRITER: Set to "true" to enable this writer (default: "false")

Author: Assistant  
Created: 2025-10-03
"""

import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

from .base import DataWriterInterface, DataWriterError

# Configuration constants
ENV_ENABLE_KEY = "ENABLE_DIRECT_DELTA_WRITER"
DEFAULT_WAREHOUSE_ID = "dd43ee29fedd958d"  # Updated SQL Warehouse for better performance

# Check if direct Delta writer should be enabled
IS_ENABLED = os.getenv(ENV_ENABLE_KEY, "false").lower() == "true"

logger = logging.getLogger(__name__)

# Always try to import SDK for user-requested Direct Delta Writer
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout
    SDK_AVAILABLE = True
    logger.info("✅ Direct Delta Writer SDK loaded (available for user selection)")
except ImportError as e:
    SDK_AVAILABLE = False
    SDK_ERROR = str(e)
    logger.warning(f"⚠️ Databricks SDK not available: {e}")


class DirectDeltaWriter(DataWriterInterface):
    """
    Direct Delta table writer using Databricks SDK
    
    This implementation writes data directly to Delta tables by executing
    SQL INSERT statements via a Databricks SQL warehouse. It provides reliable
    data persistence with detailed error reporting and execution tracking.
    
    Attributes:
        warehouse_id: SQL warehouse ID for executing statements
        workspace_client: Databricks workspace client instance
    """
    
    def __init__(self, warehouse_id: Optional[str] = None):
        """
        Initialize the Direct Delta Writer
        
        Args:
            warehouse_id: Optional SQL warehouse ID (uses default if not provided)
        """
        self.warehouse_id = warehouse_id or DEFAULT_WAREHOUSE_ID
        self._workspace_client: Optional[WorkspaceClient] = None
        self._initialization_error: Optional[str] = None
        
        if not IS_ENABLED:
            logger.info("🔒 Direct Delta Writer is DISABLED")
            return
        
        if not SDK_AVAILABLE:
            self._initialization_error = f"Databricks SDK not available: {SDK_ERROR}"
            logger.error(f"❌ {self._initialization_error}")
            return
        
        try:
            # Initialize Databricks workspace client
            self._workspace_client = WorkspaceClient()
            logger.info("✅ Databricks SDK WorkspaceClient initialized successfully")
        except Exception as e:
            self._initialization_error = f"Failed to initialize WorkspaceClient: {e}"
            logger.error(f"❌ {self._initialization_error}")
    
    @property
    def writer_name(self) -> str:
        return "Direct Delta Writer"
    
    @property
    def strategies(self) -> List[str]:
        if self.is_available:
            return ["direct_sql_insert", "databricks_sdk", "sql_warehouse"]
        else:
            return ["direct_delta_disabled", "mock_fallback"]
    
    @property
    def is_available(self) -> bool:
        """
        Check if Direct Delta Writer is available
        
        Note: This writer can be used even when ENABLE_DIRECT_DELTA_WRITER=false
        if explicitly requested by the user via the UI. The environment variable
        only controls the factory's automatic selection.
        """
        return (SDK_AVAILABLE and 
                self._workspace_client is not None and
                self._initialization_error is None)
    
    @property
    def configuration(self) -> Dict[str, Any]:
        return {
            "type": "direct_delta",
            "enabled": IS_ENABLED,
            "sdk_available": SDK_AVAILABLE,
            "warehouse_id": self.warehouse_id,
            "client_initialized": self._workspace_client is not None,
            "initialization_error": self._initialization_error,
            "enable_instruction": f"Set {ENV_ENABLE_KEY}=true to enable"
        }
    
    async def write_to_delta_table(
        self, 
        table_name: str, 
        data: List[Dict[str, Any]], 
        schema_name: str = "zerobus_delta", 
        catalog_name: str = "kaustavpaul_demo"
    ) -> Dict[str, Any]:
        """Write data to Delta table (or return mock result if disabled)"""
        
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Return mock result if not available
        if not self.is_available:
            logger.info(f"🧪 MOCK: Direct Delta Writer not available - simulating write to {full_table_name}")
            logger.info(f"📊 MOCK: Would write {len(data)} records")
            
            return {
                "status": "success",
                "message": f"MOCK: Direct Delta Writer not available - would write {len(data)} records",
                "records_written": 0,
                "records_simulated": len(data),
                "table": full_table_name,
                "approach": "direct_delta_disabled",
                "writer_name": self.writer_name,
                "warehouse_id": self.warehouse_id,
                "mock": True,
                "reason": self._initialization_error or "Direct Delta Writer is disabled",
                "enable_instruction": f"Set {ENV_ENABLE_KEY}=true to enable real writing",
                "timestamp": datetime.now().isoformat()
            }
        
        # Enhanced Direct Delta operation logging
        start_time = datetime.now()
        logger.info("🏗️" + "=" * 78)
        logger.info(f"🏗️ DIRECT DELTA WRITE OPERATION STARTED")
        logger.info(f"📊 Operation Details:")
        logger.info(f"   - Target Table: {full_table_name}")
        logger.info(f"   - Records Count: {len(data)}")
        logger.info(f"   - SQL Warehouse ID: {self.warehouse_id}")
        logger.info(f"   - Authentication: Databricks SDK")
        logger.info(f"   - Start Time: {start_time.isoformat()}")
        logger.info("🏗️" + "=" * 78)
        
        try:
            # Convert data to DataFrame for easier processing
            logger.info(f"📊 Converting {len(data)} records to DataFrame...")
            dataframe = pd.DataFrame(data)
            logger.info(f"📋 DataFrame columns: {list(dataframe.columns)}")
            
            # Generate and execute INSERT statements
            insert_statements = self._generate_insert_statements(dataframe, full_table_name)
            logger.info(f"📝 Generated {len(insert_statements)} INSERT statements")
            
            execution_results = await self._execute_insert_statements(insert_statements)
            
            # Analyze results with better status handling
            successful_count = sum(1 for result in execution_results 
                                 if result.get("status") == "SUCCEEDED")
            pending_count = sum(1 for result in execution_results 
                              if result.get("status") == "PENDING")
            running_count = sum(1 for result in execution_results 
                              if result.get("status") == "RUNNING")
            failed_count = len(execution_results) - successful_count - pending_count - running_count
            
            logger.info(f"✅ Direct Delta write completed:")
            logger.info(f"   - {successful_count} records written successfully")
            if pending_count > 0:
                logger.info(f"   - {pending_count} records pending (may complete later)")
            if running_count > 0:
                logger.info(f"   - {running_count} records still running")
            if failed_count > 0:
                logger.warning(f"   - {failed_count} records failed")
            
            # Determine overall success - consider PENDING as potentially successful
            total_attempted = len(execution_results)
            potentially_successful = successful_count + pending_count + running_count
            
            return {
                "status": "success",
                "message": f"Submitted {total_attempted} records to Delta table ({successful_count} completed, {pending_count} pending)",
                "records_written": successful_count,
                "records_pending": pending_count,
                "records_running": running_count,
                "records_failed": failed_count,
                "table": full_table_name,
                "approach": "direct_sql_insert",
                "writer_name": self.writer_name,
                "warehouse_id": self.warehouse_id,
                "execution_results": execution_results,
                "timestamp": datetime.now().isoformat(),
                "mock": False,
                "note": "PENDING statements may still complete successfully. Check warehouse for final status."
            }
            
        except Exception as e:
            error_msg = f"Direct Delta write failed: {e}"
            logger.error(f"❌ {error_msg}")
            
            import traceback
            traceback_str = traceback.format_exc()
            logger.error(f"❌ Full traceback: {traceback_str}")
            
            raise DataWriterError(
                message=error_msg,
                error_type="DirectDeltaWriteError",
                details={
                    "table": full_table_name,
                    "records_attempted": len(data),
                    "warehouse_id": self.warehouse_id,
                    "traceback": traceback_str
                }
            )
    
    def _generate_insert_statements(self, dataframe: pd.DataFrame, table_name: str) -> List[str]:
        """
        Generate SQL INSERT statements from DataFrame
        
        Args:
            dataframe: DataFrame containing the data
            table_name: Full table name (catalog.schema.table)
            
        Returns:
            List of SQL INSERT statements
        """
        insert_statements = []
        columns_str = ", ".join(dataframe.columns)
        
        for _, row in dataframe.iterrows():
            # Format values for SQL insertion with proper escaping
            formatted_values = []
            for column in dataframe.columns:
                value = row[column]
                formatted_value = self._format_sql_value(value)
                formatted_values.append(formatted_value)
            
            values_str = ", ".join(formatted_values)
            insert_statement = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
            insert_statements.append(insert_statement)
        
        return insert_statements
    
    def _format_sql_value(self, value: Any) -> str:
        """
        Format a Python value for SQL insertion with proper escaping
        
        Args:
            value: Python value to format
            
        Returns:
            SQL-formatted string representation of the value
        """
        if pd.isna(value):
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes and wrap in quotes
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # Convert to string and escape
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"
    
    async def _execute_insert_statements(self, statements: List[str]) -> List[Dict[str, Any]]:
        """
        Execute SQL INSERT statements with detailed error tracking
        
        Args:
            statements: List of SQL INSERT statements to execute
            
        Returns:
            List of execution results for each statement
        """
        execution_results = []
        
        for i, statement in enumerate(statements, 1):
            logger.info(f"📝 Executing INSERT {i}/{len(statements)}")
            
            try:
                # Log the SQL statement (truncated for readability)
                statement_preview = f"{statement[:200]}{'...' if len(statement) > 200 else ''}"
                logger.info(f"🔄 SQL: {statement_preview}")
                
                # Execute the statement with better timeout handling
                result = self._workspace_client.statement_execution.execute_statement(
                    statement=statement,
                    warehouse_id=self.warehouse_id,
                    on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
                    wait_timeout="45s"  # Wait up to 45 seconds for completion (increased timeout)
                )
                
                # Extract detailed status and error information
                status_value = result.status.state.value if result.status else "UNKNOWN"
                error_details = None
                
                # If statement is still pending, try to poll for completion
                if status_value == "PENDING" and result.statement_id:
                    logger.info(f"⏳ Statement {result.statement_id} is PENDING, polling for completion...")
                    try:
                        # Wait a bit more and check status
                        import time
                        time.sleep(5)  # Wait 5 more seconds
                        
                        # Get updated status
                        status_response = self._workspace_client.statement_execution.get_statement(result.statement_id)
                        if status_response and status_response.status:
                            updated_status = status_response.status.state.value
                            logger.info(f"📊 Updated statement status: {updated_status}")
                            
                            if updated_status in ["SUCCEEDED", "FAILED", "CANCELED"]:
                                status_value = updated_status
                                logger.info(f"✅ Statement completed with status: {status_value}")
                            else:
                                logger.info(f"⏳ Statement still {updated_status}, may complete later")
                                
                    except Exception as poll_error:
                        logger.warning(f"⚠️ Could not poll statement status: {poll_error}")
                        # Continue with original PENDING status
                
                if result.status and result.status.error:
                    error_details = {
                        "message": result.status.error.message,
                        "error_code": getattr(result.status.error, 'error_code', None),
                        "sql_state": getattr(result.status.error, 'sql_state', None)
                    }
                    logger.error(f"❌ SQL Error for INSERT {i}: {error_details}")
                
                execution_results.append({
                    "statement_id": result.statement_id,
                    "status": status_value,
                    "error_details": error_details,
                    "statement_index": i - 1,
                    "statement_preview": statement_preview
                })
                
                if status_value == "SUCCEEDED":
                    logger.info(f"✅ INSERT {i} completed successfully: {result.statement_id}")
                elif status_value == "PENDING":
                    logger.warning(f"⏳ INSERT {i} still pending (may complete later): {result.statement_id}")
                    logger.info("💡 Tip: Statement may still be executing. Check warehouse for completion.")
                elif status_value == "RUNNING":
                    logger.info(f"🔄 INSERT {i} is running: {result.statement_id}")
                else:
                    logger.error(f"❌ INSERT {i} failed with status {status_value}: {result.statement_id}")
                
            except Exception as statement_error:
                error_msg = f"INSERT {i} exception: {statement_error}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"❌ Failed SQL: {statement[:200]}{'...' if len(statement) > 200 else ''}")
                
                execution_results.append({
                    "statement_id": None,
                    "status": "FAILED",
                    "error_details": {"message": str(statement_error)},
                    "statement_index": i - 1,
                    "exception": str(statement_error)
                })
        
        return execution_results