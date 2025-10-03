"""
Writers Package - Data Writing Implementations

This package contains all data writer implementations for the Databricks app.
It follows Databricks Apps best practices for modular organization.

Modules:
    base: Abstract base classes and interfaces
    direct_delta: Direct Delta table writer via Databricks SDK  
    zerobus: Zerobus SDK writer (placeholder)
    factory: Writer factory for creating appropriate writers

Usage:
    from writers.factory import create_writer
    writer = create_writer()
    result = await writer.write_to_delta_table(...)
"""

from .base import DataWriterInterface, MockDataWriter, DataWriterError
from .factory import create_writer, get_writer_status

__all__ = [
    'DataWriterInterface',
    'MockDataWriter', 
    'DataWriterError',
    'create_writer',
    'get_writer_status'
]

__version__ = '1.0.0'
