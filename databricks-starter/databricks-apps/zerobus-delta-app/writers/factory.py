#!/usr/bin/env python3
"""
Writer Factory Module

This module provides centralized factory functions for creating data writers
following Databricks Apps best practices. It automatically selects the best
available writer based on configuration and availability.

Functions:
    create_writer: Create the best available data writer
    get_writer_status: Get status of all available writers

Author: Assistant
Created: 2025-10-03
"""

import logging
import os
from typing import Dict, Any

from .base import DataWriterInterface, MockDataWriter

logger = logging.getLogger(__name__)


def create_writer() -> DataWriterInterface:
    """
    Create the best available data writer based on configuration and availability.
    
    Selection priority:
    1. Zerobus Writer (if ENABLE_ZEROBUS_WRITER=true and available)
    2. Direct Delta Writer (if ENABLE_DIRECT_DELTA_WRITER=true and available) 
    3. Mock Writer (always available as fallback)
    
    Returns:
        DataWriterInterface: The best available data writer instance
    """
    
    # Check for Zerobus Writer first (highest priority)
    if os.getenv("ENABLE_ZEROBUS_WRITER", "false").lower() == "true":
        try:
            from .zerobus import ZerobusWriter
            writer = ZerobusWriter()
            if writer.is_available:
                logger.info("✅ Selected Zerobus Writer")
                return writer
            else:
                logger.info("⚠️ Zerobus Writer enabled but not available, trying alternatives...")
        except ImportError as e:
            logger.warning(f"⚠️ Failed to import Zerobus Writer: {e}")
    
    # Check for Direct Delta Writer second
    if os.getenv("ENABLE_DIRECT_DELTA_WRITER", "false").lower() == "true":
        try:
            from .direct_delta import DirectDeltaWriter
            writer = DirectDeltaWriter()
            if writer.is_available:
                logger.info("✅ Selected Direct Delta Writer")
                return writer
            else:
                logger.info("⚠️ Direct Delta Writer enabled but not available, falling back to mock...")
        except ImportError as e:
            logger.warning(f"⚠️ Failed to import Direct Delta Writer: {e}")
    
    # Fall back to Mock Writer (always available)
    logger.info("🧪 Selected Mock Writer (all real writers disabled)")
    return MockDataWriter()


def get_writer_status() -> Dict[str, Any]:
    """
    Get comprehensive status information about all available writers.
    
    Returns:
        Dict containing:
        - active_writer: Information about the currently selected writer
        - available_writers: Status of each writer type
        - environment_config: Relevant environment variables
    """
    status = {
        "active_writer": None,
        "available_writers": {},
        "environment_config": {
            "ENABLE_ZEROBUS_WRITER": os.getenv("ENABLE_ZEROBUS_WRITER", "false"),
            "ENABLE_DIRECT_DELTA_WRITER": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "false"),
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "Not set"),
            "DATABRICKS_TOKEN": "SET" if os.getenv("DATABRICKS_TOKEN") else "NOT SET"
        }
    }
    
    # Check Zerobus Writer status
    try:
        from .zerobus import ZerobusWriter
        writer = ZerobusWriter()
        status["available_writers"]["zerobus"] = {
            "writer_name": writer.writer_name,
            "is_available": writer.is_available,
            "strategies": writer.strategies,
            "configuration": writer.configuration,
            "enabled_via_env": os.getenv("ENABLE_ZEROBUS_WRITER", "false").lower() == "true"
        }
    except ImportError as e:
        status["available_writers"]["zerobus"] = {
            "writer_name": "Zerobus Writer",
            "is_available": False,
            "import_error": str(e),
            "enabled_via_env": os.getenv("ENABLE_ZEROBUS_WRITER", "false").lower() == "true"
        }
    
    # Check Direct Delta Writer status
    try:
        from .direct_delta import DirectDeltaWriter
        writer = DirectDeltaWriter()
        status["available_writers"]["direct_delta"] = {
            "writer_name": writer.writer_name,
            "is_available": writer.is_available,
            "strategies": writer.strategies,
            "configuration": writer.configuration,
            "enabled_via_env": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "false").lower() == "true"
        }
    except ImportError as e:
        status["available_writers"]["direct_delta"] = {
            "writer_name": "Direct Delta Writer",
            "is_available": False,
            "import_error": str(e),
            "enabled_via_env": os.getenv("ENABLE_DIRECT_DELTA_WRITER", "false").lower() == "true"
        }
    
    # Mock Writer status (always available)
    mock_writer = MockDataWriter()
    status["available_writers"]["mock"] = {
        "writer_name": mock_writer.writer_name,
        "is_available": mock_writer.is_available,
        "strategies": mock_writer.strategies,
        "configuration": mock_writer.configuration,
        "enabled_via_env": True  # Always enabled as fallback
    }
    
    # Determine active writer
    active_writer = create_writer()
    status["active_writer"] = {
        "writer_name": active_writer.writer_name,
        "writer_type": type(active_writer).__name__,
        "strategies": active_writer.strategies,
        "is_available": active_writer.is_available,
        "configuration": active_writer.configuration
    }
    
    return status


# Legacy compatibility functions for backward compatibility
def create_data_writer() -> DataWriterInterface:
    """Legacy compatibility function - use create_writer() instead"""
    logger.warning("create_data_writer() is deprecated, use create_writer() instead")
    return create_writer()


def create_zerobus_client() -> DataWriterInterface:
    """Legacy compatibility function - use create_writer() instead"""
    logger.warning("create_zerobus_client() is deprecated, use create_writer() instead")
    return create_writer()


# Custom exception for compatibility
class ZerobusException(Exception):
    """Legacy exception class for backward compatibility"""
    pass