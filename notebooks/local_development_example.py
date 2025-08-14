#!/usr/bin/env python3
"""
Local Development with Databricks Connect
This notebook demonstrates how to develop and test code locally using Databricks Connect
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Local Development with Databricks Connect
# MAGIC 
# MAGIC This notebook shows how to develop and test your Databricks code locally before running it in the workspace.

# COMMAND ----------

# Import required libraries
from databricks.connect import DatabricksSession
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Databricks Connect
# MAGIC 
# MAGIC First, let's set up the connection to your Databricks cluster.
# MAGIC 
# MAGIC **Before running this notebook, set these environment variables:**
# MAGIC ```bash
# MAGIC export DATABRICKS_HOST='your-workspace-url'
# MAGIC export DATABRICKS_CLUSTER_ID='your-cluster-id'
# MAGIC export DATABRICKS_TOKEN='your-personal-access-token'
# MAGIC export DATABRICKS_ORG_ID='your-org-id'
# MAGIC ```

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

# MAGIC %md
# MAGIC ## Create Spark Session
# MAGIC 
# MAGIC Now let's create a Spark session that connects to your Databricks cluster.

# COMMAND ----------

# Create Databricks session
spark = DatabricksSession.builder.remote().getOrCreate()

print(f"✅ Connected to Databricks cluster!")
print(f"   Spark version: {spark.version}")
print(f"   Cluster ID: {os.environ['DATABRICKS_CLUSTER_ID']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Basic Operations
# MAGIC 
# MAGIC Let's test some basic DataFrame operations to ensure everything is working.

# COMMAND ----------

# Create sample data
sample_data = [
    ("Alice", 25, "Engineer", "San Francisco"),
    ("Bob", 30, "Data Scientist", "New York"),
    ("Charlie", 35, "Manager", "Seattle"),
    ("Diana", 28, "Engineer", "Austin"),
    ("Eve", 32, "Data Scientist", "Boston")
]

# Create DataFrame
df = spark.createDataFrame(sample_data, ["name", "age", "role", "city"])

print("✅ Sample DataFrame created:")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Analysis Examples
# MAGIC 
# MAGIC Now let's perform some data analysis operations.

# COMMAND ----------

# Show schema
print("📋 DataFrame Schema:")
df.printSchema()

# COMMAND ----------

# Basic aggregations
print("📊 Role distribution:")
role_counts = df.groupBy("role").count().orderBy("count", ascending=False)
role_counts.show()

# COMMAND ----------

# Filtering
print("🔍 Engineers only:")
engineers = df.filter(df.role == "Engineer")
engineers.show()

# COMMAND ----------

# SQL operations
print("💬 SQL query example:")
df.createOrReplaceTempView("employees")
result = spark.sql("""
    SELECT role, AVG(age) as avg_age, COUNT(*) as count
    FROM employees 
    GROUP BY role 
    ORDER BY avg_age DESC
""")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Delta Tables
# MAGIC 
# MAGIC Let's demonstrate how to work with Delta tables (if they exist in your workspace).

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Available Tables
# MAGIC 
# MAGIC You can explore what tables are available in your workspace.

# COMMAND ----------

try:
    # List all tables in the default schema
    tables = spark.sql("SHOW TABLES")
    print("📚 Available tables in default schema:")
    tables.show()
except Exception as e:
    print(f"ℹ️  No tables found or access limited: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data as Delta Table
# MAGIC 
# MAGIC You can save your local DataFrames as Delta tables in your Databricks workspace.

# COMMAND ----------

# Save the sample data as a Delta table
try:
    df.write.mode("overwrite").saveAsTable("local_dev_sample_employees")
    print("✅ Successfully saved DataFrame as Delta table: local_dev_sample_employees")
    
    # Verify by reading it back
    saved_df = spark.read.table("local_dev_sample_employees")
    print("✅ Successfully read back the saved table:")
    saved_df.show()
    
except Exception as e:
    print(f"ℹ️  Could not save table (may need permissions): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC 
# MAGIC Don't forget to clean up temporary views.

# COMMAND ----------

# Clean up temporary views
spark.catalog.dropTempView("employees")
print("✅ Cleaned up temporary views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC 🎉 **Congratulations!** You've successfully set up Databricks Connect for local development.
# MAGIC 
# MAGIC ### What you can now do:
# MAGIC 
# MAGIC 1. **Develop locally**: Write and test your code in your IDE
# MAGIC 2. **Use full Databricks features**: Access your cluster's compute and data
# MAGIC 3. **Iterate quickly**: Test changes without waiting for notebook execution
# MAGIC 4. **Version control**: Use Git with your local development workflow
# MAGIC 5. **Debug effectively**: Use your IDE's debugging tools
# MAGIC 
# MAGIC ### Next steps:
# MAGIC 
# MAGIC - Import this notebook into your IDE
# MAGIC - Modify the code to work with your actual data
# MAGIC - Use the `spark` object to access your workspace tables
# MAGIC - Develop your data pipelines locally before deploying
# MAGIC 
# MAGIC ### Remember:
# MAGIC - Keep your cluster running while developing locally
# MAGIC - The connection uses your cluster's compute resources
# MAGIC - You can access all the same libraries and configurations as in the workspace
