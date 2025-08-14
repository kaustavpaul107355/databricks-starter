"""
Databricks Configuration

This file contains configuration settings for connecting to your Databricks workspace.
Copy this file to .env and fill in your actual values.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Databricks Workspace Configuration
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'your-workspace.cloud.databricks.com')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', 'your-personal-access-token')
DATABRICKS_CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID', 'your-cluster-id')
DATABRICKS_ORG_ID = os.getenv('DATABRICKS_ORG_ID', 'your-org-id')
DATABRICKS_PORT = os.getenv('DATABRICKS_PORT', '15001')

# Database and Schema Configuration
CATALOG_NAME = os.getenv('CATALOG_NAME', 'hive_metastore')
SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'default')

# Storage Configuration
STORAGE_ACCOUNT = os.getenv('STORAGE_ACCOUNT', 'your-storage-account')
CONTAINER_NAME = os.getenv('CONTAINER_NAME', 'your-container')

def get_databricks_config():
    """Return Databricks configuration as a dictionary."""
    return {
        'host': DATABRICKS_HOST,
        'token': DATABRICKS_TOKEN,
        'cluster_id': DATABRICKS_CLUSTER_ID,
        'org_id': DATABRICKS_ORG_ID,
        'port': DATABRICKS_PORT,
        'catalog': CATALOG_NAME,
        'schema': SCHEMA_NAME
    }

def validate_config():
    """Validate that required configuration values are set."""
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Warning: Missing required environment variables: {missing_vars}")
        print("Please set these in your .env file")
        return False
    
    return True

if __name__ == "__main__":
    validate_config()
