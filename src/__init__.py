"""
Databricks Starter Package

A starter template for Databricks development projects.
"""

__version__ = "0.1.0"
__author__ = "Kaustav Paul"
__email__ = "kaustav.paul@databricks.com"

# Import utility functions
from .databricks_utils import (
    create_spark_session,
    read_delta_table,
    write_delta_table,
    add_audit_columns,
    validate_dataframe,
    get_table_info
)

# Import user folder management
from .user_folder_manager import (
    UserFolderManager,
    create_user_folder_manager
)

__all__ = [
    # Core utilities
    "create_spark_session",
    "read_delta_table", 
    "write_delta_table",
    "add_audit_columns",
    "validate_dataframe",
    "get_table_info",
    
    # User folder management
    "UserFolderManager",
    "create_user_folder_manager"
]
