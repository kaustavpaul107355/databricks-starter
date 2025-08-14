#!/usr/bin/env python3
"""
Simple Example Notebook

This notebook demonstrates basic operations that can be run on your Databricks cluster.
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Simple Example Notebook
# MAGIC 
# MAGIC This notebook shows basic operations you can perform on your Databricks cluster.

# COMMAND ----------

# Import required libraries
from databricks.connect import DatabricksSession
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Databricks
# MAGIC 
# MAGIC First, let's establish a connection to your cluster.

# COMMAND ----------

# Create Databricks session
spark = DatabricksSession.builder.remote().getOrCreate()

print(f"✅ Connected to Databricks cluster!")
print(f"   Spark version: {spark.version}")
print(f"   Cluster ID: {os.environ.get('DATABRICKS_CLUSTER_ID', 'Unknown')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Data Operations
# MAGIC 
# MAGIC Let's perform some basic data operations to test the connection.

# COMMAND ----------

# Create sample data
print("📝 Creating sample data...")

sample_data = [
    ("Project A", 100, "Active"),
    ("Project B", 250, "Completed"),
    ("Project C", 75, "Planning"),
    ("Project D", 300, "Active"),
    ("Project E", 150, "Completed")
]

# Create DataFrame
df = spark.createDataFrame(sample_data, ["project_name", "budget", "status"])

print("✅ Sample DataFrame created:")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Analysis
# MAGIC 
# MAGIC Now let's perform some analysis on the data.

# COMMAND ----------

# Show schema
print("📋 DataFrame Schema:")
df.printSchema()

# COMMAND ----------

# Basic statistics
print("📊 Basic Statistics:")
df.describe().show()

# COMMAND ----------

# Filter data
print("🔍 Active Projects:")
active_projects = df.filter(df.status == "Active")
active_projects.show()

# COMMAND ----------

# Aggregations
print("💰 Budget by Status:")
budget_by_status = df.groupBy("status").agg(
    {"budget": "sum", "project_name": "count"}
).withColumnRenamed("sum(budget)", "total_budget").withColumnRenamed("count(project_name)", "project_count")

budget_by_status.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results
# MAGIC 
# MAGIC Let's save the results to your user folder.

# COMMAND ----------

# Save as Parquet - using the correct path format
output_path = "/Users/kaustav.paul@databricks.com/project_analysis.parquet"
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Results saved to: {output_path}")

# Also save as CSV for easier viewing
csv_path = "/Users/kaustav.paul@databricks.com/project_analysis.csv"
df.write.mode("overwrite").csv(csv_path, header=True)

print(f"✅ Results also saved as CSV to: {csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook successfully:
# MAGIC - ✅ Connected to your Databricks cluster
# MAGIC - ✅ Created and analyzed sample data
# MAGIC - ✅ Performed data transformations
# MAGIC - ✅ Saved results to your user folder
# MAGIC 
# MAGIC You can now run this notebook on your cluster using:
# MAGIC ```bash
# MAGIC python run_notebook.py notebooks/simple_example.py
# MAGIC ```

print("🎉 Notebook execution completed successfully!")
print("📁 Check your user folder for the saved results:")
print(f"   {output_path}")
print(f"   {csv_path}")
