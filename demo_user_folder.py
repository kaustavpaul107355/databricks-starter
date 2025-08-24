#!/usr/bin/env python3
"""
User Folder Management Demo
===========================

Demonstrates the user folder management capabilities.
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass  # Continue without dotenv if not available
except Exception:
    pass  # Continue if .env loading fails

from databricks.connect import DatabricksSession
from src.user_folder_manager import create_user_folder_manager

def setup_environment():
    """Set up and verify the environment."""
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables:")
        print("export DATABRICKS_HOST='your-workspace-url'")
        print("export DATABRICKS_CLUSTER_ID='your-cluster-id'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return False
    
    print("✅ Environment variables found")
    return True

def main():
    """Main demonstration function."""
    print("🚀 User Folder Management Demo")
    print("=" * 50)
    
    # Setup environment
    if not setup_environment():
        return
    
    try:
        # Create Databricks session
        print("\n🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Create user folder manager
        print("\n📁 Setting up User Folder Manager...")
        user_manager = create_user_folder_manager(spark)
        print(f"✅ Managing folder: {user_manager.user_folder}")
        
        # Get folder information
        print("\n🔍 Getting folder information...")
        folder_info = user_manager.get_folder_info()
        print(f"📊 Folder: {folder_info['user_folder']}")
        print(f"📊 Total files: {folder_info['total_files']}")
        print(f"📊 File types: {folder_info['file_types']}")
        
        # Create test data
        print("\n📝 Creating test data...")
        test_data = [
            ("demo_file_1", "Sample data 1", 100),
            ("demo_file_2", "Sample data 2", 200),
            ("demo_file_3", "Sample data 3", 300)
        ]
        schema = ["name", "description", "value"]
        
        test_file_path = user_manager.create_test_data("demo_data.parquet", test_data, schema)
        print(f"✅ Created test data at: {test_file_path}")
        
        # Read the test data
        print("\n📖 Reading test data...")
        df = user_manager.read_file("demo_data.parquet")
        print("📊 Test data contents:")
        df.show()
        
        # List files again to see the new file
        print("\n📁 Updated file list:")
        files_df = user_manager.list_files()
        if files_df.count() > 0:
            files_df.select("path", "length").show(truncate=False)
        
        # Demonstrate file operations
        print("\n🔄 Demonstrating file operations...")
        
        # Write as CSV
        csv_path = user_manager.write_file(df, "demo_data.csv", "csv")
        print(f"✅ Wrote CSV file: {csv_path}")
        
        # Write as JSON
        json_path = user_manager.write_file(df, "demo_data.json", "json")
        print(f"✅ Wrote JSON file: {json_path}")
        
        # Final folder info
        print("\n📊 Final folder status:")
        final_info = user_manager.get_folder_info()
        print(f"📁 Total files: {final_info['total_files']}")
        print(f"📁 File types: {final_info['file_types']}")
        
        print("\n🎉 Demo completed successfully!")
        print("\n💡 You can now:")
        print("   - Access your user folder at:", user_manager.user_folder)
        print("   - Create, read, and modify files")
        print("   - Build data pipelines")
        print("   - Manage your workspace assets")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
