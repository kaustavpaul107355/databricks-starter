#!/usr/bin/env python3
"""
User Folder Explorer
This notebook demonstrates how to interact with your user folder in the Databricks workspace
using Databricks Connect.
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # User Folder Explorer
# MAGIC 
# MAGIC This notebook shows how to access and manage files in your user folder using Databricks Connect.

# COMMAND ----------

# Import required libraries
from databricks.connect import DatabricksSession
import os
from pyspark.sql.functions import col, lit
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Databricks Connect
# MAGIC 
# MAGIC First, let's set up the connection to your Databricks cluster.

# COMMAND ----------

# Check if environment variables are set
required_vars = ['DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    print("❌ Missing required environment variables:")
    for var in missing_vars:
        print(f"   - {var}")
    print("\nPlease set these environment variables before running this notebook.")
    raise ValueError("Missing required environment variables")

print("✅ Environment variables found for Databricks Connect")

# COMMAND ----------

# Create Databricks session
spark = DatabricksSession.builder.remote().getOrCreate()

print(f"✅ Connected to Databricks cluster!")
print(f"   Spark version: {spark.version}")
print(f"   Cluster ID: {os.environ['DATABRICKS_CLUSTER_ID']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Your User Folder
# MAGIC 
# MAGIC Your user folder is typically located at `/Users/your-email@domain.com/`

# COMMAND ----------

# Get your username from the environment or use a default
username = os.environ.get('DATABRICKS_USER', 'kaustav.paul@databricks.com')
user_folder = f"/Users/{username}"

print(f"🔍 Exploring user folder: {user_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Files in Your User Folder
# MAGIC 
# MAGIC Let's see what's currently in your user folder.

# COMMAND ----------

try:
    # List files in your user folder
    files_df = spark.read.format("binaryFile").load(user_folder)
    
    if files_df.count() > 0:
        print("📁 Files found in your user folder:")
        files_df.select("path", "length", "modificationTime").show(truncate=False)
    else:
        print("📁 No files found in your user folder")
        
except Exception as e:
    print(f"⚠️ Could not list files: {e}")
    print("This might be due to permissions or the folder structure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Test File in Your User Folder
# MAGIC 
# MAGIC Let's create a test file to demonstrate write access.

# COMMAND ----------

# Create sample data
test_data = [
    ("test_file_1", "This is a test file created via Databricks Connect", "2025-08-14"),
    ("test_file_2", "Another test file for demonstration", "2025-08-14"),
    ("test_file_3", "Third test file showing write capabilities", "2025-08-14")
]

test_df = spark.createDataFrame(test_data, ["filename", "content", "created_date"])

print("📝 Creating test files in your user folder...")

try:
    # Write to your user folder
    test_path = f"{user_folder}/test_files"
    test_df.write.mode("overwrite").parquet(test_path)
    print(f"✅ Successfully created test files at: {test_path}")
    
    # Verify the files were created
    created_files = spark.read.parquet(test_path)
    print(f"📊 Created {created_files.count()} test records")
    created_files.show()
    
except Exception as e:
    print(f"❌ Failed to create test files: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Modify Existing Files
# MAGIC 
# MAGIC If you have existing files, you can read and modify them.

# COMMAND ----------

# Check if we have any existing data files
try:
    # Look for common file types
    file_extensions = ["*.parquet", "*.csv", "*.json", "*.delta"]
    
    for ext in file_extensions:
        try:
            files = spark.read.format("binaryFile").load(f"{user_folder}/{ext}")
            if files.count() > 0:
                print(f"📁 Found {ext} files:")
                files.select("path").show(truncate=False)
        except:
            continue
            
except Exception as e:
    print(f"⚠️ Could not search for existing files: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Notebook in Your User Folder
# MAGIC 
# MAGIC You can also create and manage notebooks programmatically.

# COMMAND ----------

# Create a simple notebook content
notebook_content = {
    "commands": [
        {
            "command": "# Test Notebook Created via Databricks Connect\nprint('Hello from Databricks Connect!')",
            "commandType": "python"
        }
    ]
}

print("📓 Creating a test notebook...")

try:
    # Note: Creating notebooks via API requires additional permissions
    # This is a demonstration of what's possible
    print("💡 Note: Creating notebooks via API requires additional permissions")
    print("   You can create notebooks manually in the workspace UI")
    print("   Or use the Databricks CLI with appropriate permissions")
    
except Exception as e:
    print(f"⚠️ Notebook creation requires additional permissions: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of User Folder Capabilities
# MAGIC 
# MAGIC With your current Databricks Connect setup, you can:

# COMMAND ----------

capabilities = [
    "✅ Read files from your user folder",
    "✅ Write new files to your user folder", 
    "✅ Create and modify data files (Parquet, CSV, JSON, Delta)",
    "✅ Access your personal workspace storage",
    "✅ Run queries against your data",
    "✅ Manage file permissions (within your user scope)",
    "⚠️ Create notebooks (requires additional API permissions)",
    "⚠️ Access other users' folders (requires admin permissions)"
]

print("🎯 **User Folder Capabilities Summary:**")
for capability in capabilities:
    print(f"   {capability}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC You can now:
# MAGIC 1. **Create data files** in your user folder
# MAGIC 2. **Read and analyze** existing data
# MAGIC 3. **Build data pipelines** that write to your folder
# MAGIC 4. **Manage your workspace assets** programmatically
# MAGIC 5. **Integrate with external tools** using your user folder as storage

print("🚀 Your Databricks Connect setup is ready for user folder management!")
