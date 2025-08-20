#!/usr/bin/env python3
"""
Local test script for Multi-Agent Supervisor Chat App

This script tests the core functionality of the app without starting the full Streamlit interface.
"""

import os
import sys
from mlflow.deployments import get_deploy_client

def test_mlflow_connection():
    """Test MLflow deployment client connection."""
    try:
        client = get_deploy_client('databricks')
        print("✅ MLflow deployment client created successfully")
        return client
    except Exception as e:
        print(f"❌ Failed to create MLflow deployment client: {e}")
        return None

def test_endpoint_listing(client):
    """Test listing available endpoints."""
    try:
        endpoints = client.list_endpoints()
        print(f"✅ Successfully listed {len(endpoints)} endpoints")
        for ep in endpoints:
            print(f"   - {ep['name']} ({ep.get('endpoint_type', 'unknown')})")
        return endpoints
    except Exception as e:
        print(f"❌ Failed to list endpoints: {e}")
        return []

def test_mas_endpoint_connection(client, endpoint_name):
    """Test connection to specific MAS endpoint."""
    try:
        # Try to get endpoint info
        endpoint_info = client.get_endpoint(endpoint_name)
        print(f"✅ Successfully connected to endpoint: {endpoint_name}")
        print(f"   - Status: {endpoint_info.get('state', 'unknown')}")
        print(f"   - Type: {endpoint_info.get('endpoint_type', 'unknown')}")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to endpoint {endpoint_name}: {e}")
        return False

def test_environment_variables():
    """Test environment variable configuration."""
    required_vars = ['SERVING_ENDPOINT', 'DATABRICKS_HOST']
    optional_vars = ['WORKSPACE_ID']
    
    print("🔍 Checking environment variables...")
    
    missing_required = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")
            missing_required.append(var)
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"⚠️  {var}: Not set (optional)")
    
    return len(missing_required) == 0

def main():
    """Run all tests."""
    print("🧪 Testing Multi-Agent Supervisor Chat App...")
    print("=" * 50)
    
    # Test environment variables
    env_ok = test_environment_variables()
    print()
    
    if not env_ok:
        print("❌ Environment variables not properly configured")
        print("Please set the required environment variables:")
        print("export SERVING_ENDPOINT='your-mas-endpoint'")
        print("export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'")
        sys.exit(1)
    
    # Test MLflow connection
    client = test_mlflow_connection()
    if not client:
        print("❌ MLflow connection failed")
        print("Please check your Databricks CLI configuration:")
        print("databricks configure")
        sys.exit(1)
    print()
    
    # Test endpoint listing
    endpoints = test_endpoint_listing(client)
    print()
    
    # Test MAS endpoint connection
    mas_endpoint = os.getenv('SERVING_ENDPOINT')
    if mas_endpoint:
        endpoint_ok = test_mas_endpoint_connection(client, mas_endpoint)
        if not endpoint_ok:
            print(f"⚠️  Warning: MAS endpoint {mas_endpoint} is not accessible")
            print("   This may prevent the app from working properly")
    print()
    
    print("🎉 Testing completed!")
    print()
    
    if env_ok and client and endpoints:
        print("✅ All core tests passed!")
        print("🚀 You can now run the app with: streamlit run app.py")
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
        print("📚 See README.md for setup instructions")

if __name__ == "__main__":
    main()
