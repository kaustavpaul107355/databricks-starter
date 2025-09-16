#!/usr/bin/env python3
"""
Databricks Connect Setup for Adventure Works Notebook
This script sets up environment variables for Databricks Connect
"""

import os

def setup_databricks_connect():
    """Configure Databricks Connect environment"""
    
    # Set environment variables
    os.environ['DATABRICKS_CONFIG_PROFILE'] = 'DEFAULT'
    os.environ['DATABRICKS_CLUSTER_ID'] = '0312-222653-nqfcg6yd'  # Kaustav Paul's ML Compute
    
    print("🔧 Databricks Connect Configuration:")
    print(f"  Profile: {os.environ['DATABRICKS_CONFIG_PROFILE']}")
    print(f"  Cluster: {os.environ['DATABRICKS_CLUSTER_ID']} (Kaustav Paul's ML Compute)")
    print(f"  Runtime: DBR 16.4 (Spark 3.5.2)")
    print("✅ Environment configured!")
    
    # Test connection
    try:
        from databricks.connect import DatabricksSession
        
        spark = DatabricksSession.builder.getOrCreate()
        print(f"✅ Connected to Databricks! Spark version: {spark.version}")
        
        # Return the spark session for use
        return spark
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return None

if __name__ == "__main__":
    spark = setup_databricks_connect()
    
    if spark:
        print("\n🎉 Ready to run your Adventure Works notebook!")
        print("You can now use the 'spark' object to run SQL commands on your Databricks cluster.")
        
        # Example query
        df = spark.sql("SELECT current_timestamp() as timestamp, 'Adventure Works Setup Complete!' as message")
        result = df.collect()
        print(f"\n📊 Test query result: {result[0]['message']} at {result[0]['timestamp']}")
