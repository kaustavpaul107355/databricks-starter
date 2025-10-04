#!/usr/bin/env python3
"""
Data Writer Interface Module

This module defines the abstract base class and interfaces for data writing implementations.
It provides a clean contract that all data writers must implement, enabling easy
swapping between different implementations (Direct Delta, Zerobus, Mock, etc.).

=== ARCHITECTURE PATTERN ===

This module implements the Strategy Pattern for data writing:

┌─────────────────────┐
│  DataWriterInterface │  ← Abstract base class
│  (ABC)              │
└─────────────────────┘
          ▲
          │ implements
    ┌─────┴─────┬─────────────┬──────────────┐
    │           │             │              │
┌───▼────┐ ┌───▼────┐ ┌──────▼──────┐ ┌────▼─────┐
│ Mock   │ │ Direct │ │   Zerobus   │ │  Future  │
│Writer  │ │ Delta  │ │   Writer    │ │ Writers  │
│        │ │Writer  │ │             │ │          │
└────────┘ └────────┘ └─────────────┘ └──────────┘

=== KEY FEATURES ===

🔧 Abstract Interface:
   - Consistent API across all writer implementations
   - Type hints and comprehensive documentation
   - Standardized error handling and logging

🎯 Strategy Pattern Benefits:
   - Easy to add new writer implementations
   - Runtime writer selection based on configuration
   - Clean separation of concerns
   - Testable and mockable interfaces

📊 Comprehensive Metadata:
   - Writer identification and capabilities
   - Configuration information
   - Availability status checking
   - Performance and strategy reporting

Classes:
    DataWriterError: Custom exception with detailed error context
    DataWriterInterface: Abstract base class defining the writer contract
    MockDataWriter: Safe testing implementation with simulation capabilities

Author: Assistant
Created: 2025-10-03
Updated: 2025-10-03 - Enhanced documentation and architecture overview
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataWriterError(Exception):
    """Custom exception for data writer errors"""
    
    def __init__(self, message: str, error_type: str = "DataWriterError", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.error_type = error_type
        self.details = details or {}
        self.timestamp = datetime.now().isoformat()


class DataWriterInterface(ABC):
    """
    Abstract base class for all data writers
    
    This interface defines the contract that all data writer implementations must follow.
    It ensures consistent behavior across different writing strategies while allowing
    for implementation-specific optimizations.
    """
    
    @property
    @abstractmethod
    def writer_name(self) -> str:
        """Return the human-readable name of this writer"""
        pass
    
    @property
    @abstractmethod
    def strategies(self) -> List[str]:
        """Return list of strategies/technologies this writer uses"""
        pass
    
    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if this writer is available and properly configured"""
        pass
    
    @property
    @abstractmethod
    def configuration(self) -> Dict[str, Any]:
        """Return configuration information for this writer"""
        pass
    
    @abstractmethod
    async def write_to_delta_table(
        self, 
        table_name: str, 
        data: List[Dict[str, Any]], 
        schema_name: str = "zerobus_delta", 
        catalog_name: str = "kaustavpaul_demo"
    ) -> Dict[str, Any]:
        """
        Write data to Delta table
        
        Args:
            table_name: Name of the target table (without catalog/schema prefix)
            data: List of dictionaries containing the data to write
            schema_name: Schema name (default: "zerobus_delta")
            catalog_name: Catalog name (default: "kaustavpaul_demo")
            
        Returns:
            Dictionary containing write results and status with keys:
            - status: "success" or "failed"
            - message: Human-readable message
            - records_written: Number of records successfully written
            - table: Full table name (catalog.schema.table)
            - approach: Writer implementation used
            - mock: Boolean indicating if this was a mock operation
            - timestamp: ISO timestamp of operation
            
        Raises:
            DataWriterError: If writing fails with unrecoverable error
        """
        pass
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on this writer
        
        Returns:
            Dictionary containing health status information
        """
        return {
            "writer_name": self.writer_name,
            "is_available": self.is_available,
            "strategies": self.strategies,
            "configuration": self.configuration,
            "timestamp": datetime.now().isoformat()
        }


class MockDataWriter(DataWriterInterface):
    """
    Mock data writer for testing and fallback scenarios
    
    This implementation simulates data writing operations without actually
    persisting data. It's useful for testing, development, and as a fallback
    when no real writers are available.
    """
    
    def __init__(self):
        """Initialize the mock data writer"""
        self._records_simulated = 0
        logger.info("🧪 Mock Data Writer initialized")
    
    @property
    def writer_name(self) -> str:
        return "Mock Data Writer"
    
    @property
    def strategies(self) -> List[str]:
        return ["mock_simulation", "testing", "fallback"]
    
    @property
    def is_available(self) -> bool:
        return True  # Mock writer is always available
    
    @property
    def configuration(self) -> Dict[str, Any]:
        return {
            "type": "mock",
            "records_simulated": self._records_simulated,
            "always_available": True,
            "purpose": "Testing and fallback"
        }
    
    async def write_to_delta_table(
        self, 
        table_name: str, 
        data: List[Dict[str, Any]], 
        schema_name: str = "zerobus_delta", 
        catalog_name: str = "kaustavpaul_demo"
    ) -> Dict[str, Any]:
        """Mock implementation - simulates writing without persisting data"""
        
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        logger.info(f"🧪 MOCK: Simulating write to table: {full_table_name}")
        logger.info(f"📊 MOCK: Simulating write of {len(data)} records")
        
        # Simulate processing each record
        for i, record in enumerate(data, 1):
            product_id = record.get('product_id', f'UNKNOWN_{i}')
            logger.info(f"📝 MOCK: Simulating write of record {i}: {product_id}")
        
        # Update simulation counter
        self._records_simulated += len(data)
        
        return {
            "status": "success",
            "message": f"MOCK: Successfully simulated writing {len(data)} records",
            "records_written": 0,  # No actual records written
            "records_simulated": len(data),
            "table": full_table_name,
            "approach": "mock_simulation",
            "writer_name": self.writer_name,
            "mock": True,
            "reason": "Using mock writer - no data actually persisted",
            "strategies": self.strategies,
            "timestamp": datetime.now().isoformat()
        }