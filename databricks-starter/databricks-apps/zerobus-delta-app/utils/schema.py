#!/usr/bin/env python3
"""
Schema Utilities Module

This module provides utilities for handling Delta table schema validation
and data transformation to ensure compatibility between processed data
and target table schemas.

Functions:
    validate_and_transform_data: Validate and transform data to match table schema
    get_table_schema: Get table schema information (placeholder)
    filter_data_to_schema: Filter data to only include valid columns

Author: Assistant
Created: 2025-10-03
"""

import logging
from typing import List, Dict, Any, Set, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Known table schemas - can be extended or made dynamic
KNOWN_TABLE_SCHEMAS = {
    "kaustavpaul_demo.zerobus_delta.zerobus_products_data": {
        "required_columns": [
            "record_id", "product_id", "product_name", "product_price", 
            "sale_start_date", "sale_stop_date"
        ],
        "optional_columns": [
            "category", "batch_id", "processed_at", "source"
        ]
    },
    "kaustavpaul_demo.delta_app.delta_products_data": {
        "required_columns": [
            "record_id", "product_id", "product_name", "product_price", 
            "category", "sale_start_date", "sale_stop_date"
        ],
        "optional_columns": [
            "batch_id", "processed_at", "source"
        ]
    }
}

def validate_and_transform_data(
    data: List[Dict[str, Any]], 
    table_name: str,
    strict_mode: bool = False
) -> List[Dict[str, Any]]:
    """
    Validate and transform data to match the target table schema
    
    Args:
        data: List of data records to validate and transform
        table_name: Full table name (catalog.schema.table)
        strict_mode: If True, raise errors on schema mismatches. If False, filter gracefully.
        
    Returns:
        List of validated and transformed data records
        
    Raises:
        ValueError: If strict_mode is True and schema validation fails
    """
    
    if not data:
        logger.warning("No data provided for validation")
        return []
    
    logger.info(f"🔍 Validating data for table: {table_name}")
    logger.info(f"📊 Records to validate: {len(data)}")
    
    # Get known schema or infer from data
    schema_info = KNOWN_TABLE_SCHEMAS.get(table_name)
    
    if schema_info:
        logger.info(f"✅ Found known schema for table: {table_name}")
        required_columns = set(schema_info["required_columns"])
        optional_columns = set(schema_info["optional_columns"])
        allowed_columns = required_columns.union(optional_columns)
    else:
        logger.warning(f"⚠️ No known schema for table: {table_name}, using permissive mode")
        # In permissive mode, allow all columns from the data
        all_columns = set()
        for record in data:
            all_columns.update(record.keys())
        allowed_columns = all_columns
        required_columns = set()
        optional_columns = all_columns
    
    logger.info(f"📋 Required columns: {sorted(required_columns)}")
    logger.info(f"📋 Optional columns: {sorted(optional_columns)}")
    
    validated_data = []
    schema_issues = []
    
    for i, record in enumerate(data):
        try:
            validated_record = validate_single_record(
                record, required_columns, allowed_columns, i
            )
            validated_data.append(validated_record)
        except ValueError as e:
            schema_issues.append(f"Record {i}: {e}")
            if strict_mode:
                raise ValueError(f"Schema validation failed for record {i}: {e}")
            else:
                logger.warning(f"⚠️ Skipping invalid record {i}: {e}")
    
    if schema_issues:
        logger.warning(f"⚠️ Schema issues found: {len(schema_issues)}")
        for issue in schema_issues[:5]:  # Log first 5 issues
            logger.warning(f"   - {issue}")
        if len(schema_issues) > 5:
            logger.warning(f"   - ... and {len(schema_issues) - 5} more issues")
    
    logger.info(f"✅ Validation complete: {len(validated_data)}/{len(data)} records valid")
    return validated_data

def validate_single_record(
    record: Dict[str, Any], 
    required_columns: Set[str], 
    allowed_columns: Set[str],
    record_index: int
) -> Dict[str, Any]:
    """
    Validate and clean a single data record
    
    Args:
        record: Single data record to validate
        required_columns: Set of required column names
        allowed_columns: Set of allowed column names
        record_index: Index of record for error reporting
        
    Returns:
        Validated and cleaned record
        
    Raises:
        ValueError: If required columns are missing
    """
    
    # Check for missing required columns
    record_columns = set(record.keys())
    missing_required = required_columns - record_columns
    
    if missing_required:
        raise ValueError(f"Missing required columns: {sorted(missing_required)}")
    
    # Filter out columns not allowed in the schema
    extra_columns = record_columns - allowed_columns
    if extra_columns:
        logger.debug(f"🔧 Removing extra columns from record {record_index}: {sorted(extra_columns)}")
    
    # Create cleaned record with only allowed columns
    cleaned_record = {
        col: value for col, value in record.items() 
        if col in allowed_columns
    }
    
    # Validate and clean data types
    cleaned_record = clean_data_types(cleaned_record)
    
    return cleaned_record

def clean_data_types(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean and standardize data types in a record
    
    Args:
        record: Data record to clean
        
    Returns:
        Record with cleaned data types
    """
    
    cleaned = {}
    
    for column, value in record.items():
        if value is None:
            cleaned[column] = None
        elif column in ["product_price"]:
            # Ensure numeric fields are properly typed
            try:
                cleaned[column] = float(value)
            except (ValueError, TypeError):
                logger.warning(f"⚠️ Invalid price value for {column}: {value}, setting to 0.0")
                cleaned[column] = 0.0
        elif column in ["processed_at"]:
            # Ensure timestamp fields are strings in ISO format
            if isinstance(value, datetime):
                cleaned[column] = value.isoformat()
            else:
                cleaned[column] = str(value)
        else:
            # Convert other fields to strings
            cleaned[column] = str(value)
    
    return cleaned

def add_table_schema(table_name: str, required_columns: List[str], optional_columns: List[str]):
    """
    Add a new table schema to the known schemas
    
    Args:
        table_name: Full table name (catalog.schema.table)
        required_columns: List of required column names
        optional_columns: List of optional column names
    """
    
    KNOWN_TABLE_SCHEMAS[table_name] = {
        "required_columns": required_columns,
        "optional_columns": optional_columns
    }
    
    logger.info(f"✅ Added schema for table: {table_name}")
    logger.info(f"📋 Required: {required_columns}")
    logger.info(f"📋 Optional: {optional_columns}")

def get_schema_info(table_name: str) -> Optional[Dict[str, Any]]:
    """
    Get schema information for a table
    
    Args:
        table_name: Full table name (catalog.schema.table)
        
    Returns:
        Schema information dictionary or None if not found
    """
    
    return KNOWN_TABLE_SCHEMAS.get(table_name)
