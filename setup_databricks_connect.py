#!/usr/bin/env python3
"""
Setup and test Databricks Connect connection
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from the same directory as this script
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)
    print(f"✅ Loaded environment from: {env_path}")
except ImportError:
    print("⚠️  python-dotenv not installed, using system environment variables")
except Exception as e:
    print(f"⚠️  Could not load .env file: {e}")

from databricks.connect import DatabricksSession

def setup_databricks_connect():
    """Set up Databricks Connect configuration"""
    
    # Check if environment variables are set
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
        print("export DATABRICKS_ORG_ID='your-org-id'")
        return False
    
    print("✅ Databricks Connect environment variables found:")
    print(f"   Host: {os.environ['DATABRICKS_HOST']}")
    print(f"   Cluster ID: {os.environ['DATABRICKS_CLUSTER_ID']}")
    print(f"   Org ID: {os.environ.get('DATABRICKS_ORG_ID', 'Not set')}")
    print()
    return True

def test_connection():
    """Test the Databricks Connect connection"""
    
    try:
        print("🔄 Testing Databricks Connect connection...")
        
        # Create Databricks session
        spark = DatabricksSession.builder.remote().getOrCreate()
        
        print("✅ Successfully connected to Databricks!")
        print(f"   Spark version: {spark.version}")
        
        # Test a simple operation
        print("\n🧪 Testing basic operations...")
        
        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        print("✅ Successfully created DataFrame:")
        df.show()
        
        # Test count
        count = df.count()
        print(f"✅ DataFrame count: {count}")
        
        # Test SQL
        print("\n🧪 Testing SQL operations...")
        df.createOrReplaceTempView("test_people")
        result = spark.sql("SELECT name, age FROM test_people WHERE age > 25")
        print("✅ SQL query result:")
        result.show()
        
        return spark
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def main():
    """Main function"""
    print("🚀 Setting up Databricks Connect...\n")
    
    # Setup environment
    if not setup_databricks_connect():
        return None
    
    # Test connection
    spark = test_connection()
    
    if spark:
        print("\n🎉 Databricks Connect is working perfectly!")
        print("\nYou can now:")
        print("1. Run your notebooks locally with this connection")
        print("2. Use the 'spark' object in your Python scripts")
        print("3. Access your Databricks cluster from your IDE")
        print("4. Execute SQL queries and DataFrame operations")
        
        # Keep the session alive for interactive use
        print("\n💡 The Spark session is ready for use!")
        print("\n📝 Example usage:")
        print("   from databricks.connect import DatabricksSession")
        print("   spark = DatabricksSession.builder.remote().getOrCreate()")
        print("   df = spark.read.table('your_table_name')")
        
        return spark
    else:
        print("\n❌ Setup failed. Please check your configuration.")
        return None

if __name__ == "__main__":
    main()
