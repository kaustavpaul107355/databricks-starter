#!/usr/bin/env python3
"""
IDE Integration Demo with Databricks Connect

This notebook is designed to run directly in your IDE (Cursor) while executing on your remote Databricks cluster.
You can run individual cells, debug, and use all IDE features!
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # IDE Integration Demo
# MAGIC 
# MAGIC **Run this notebook directly in Cursor!** 🚀
# MAGIC 
# MAGIC Each cell below can be executed individually, and the code runs on your remote Databricks cluster
# MAGIC while you develop locally in your IDE.

# COMMAND ----------

# Import required libraries
from databricks.connect import DatabricksSession
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, when, lit, sum as spark_sum, count as spark_count
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Connect to Your Databricks Cluster
# MAGIC 
# MAGIC This cell establishes the connection to your remote cluster.

# COMMAND ----------

# Create Databricks session - this connects to your remote cluster
spark = DatabricksSession.builder.remote().getOrCreate()

print("🎉 Successfully connected to your Databricks cluster!")
print(f"   🌐 Workspace: {os.environ.get('DATABRICKS_HOST', 'Connected via config')}")
print(f"   🔧 Cluster: {os.environ.get('DATABRICKS_CLUSTER_ID', 'Connected via config')}")
print(f"   ⚡ Spark Version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Create Sample Data
# MAGIC 
# MAGIC Let's create some sample data to work with.

# COMMAND ----------

# Create sample sales data
print("📝 Creating sample sales data...")

sales_data = [
    ("Product A", "Electronics", 150.00, "2025-01-15", "Active"),
    ("Product B", "Clothing", 89.99, "2025-01-16", "Active"),
    ("Product C", "Electronics", 299.99, "2025-01-17", "Active"),
    ("Product D", "Home", 199.50, "2025-01-18", "Discontinued"),
    ("Product E", "Electronics", 599.99, "2025-01-19", "Active"),
    ("Product F", "Clothing", 45.00, "2025-01-20", "Active"),
    ("Product G", "Home", 129.99, "2025-01-21", "Active"),
    ("Product H", "Electronics", 899.99, "2025-01-22", "Active")
]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, ["product_name", "category", "price", "date", "status"])

print("✅ Sales DataFrame created:")
sales_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Data Exploration
# MAGIC 
# MAGIC Explore the data structure and content.

# COMMAND ----------

# Show schema
print("📋 DataFrame Schema:")
sales_df.printSchema()

# COMMAND ----------

# Basic statistics
print("📊 Price Statistics:")
sales_df.describe("price").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Data Analysis
# MAGIC 
# MAGIC Perform some analysis on the sales data.

# COMMAND ----------

# Category analysis
print("🏷️ Sales by Category:")
category_analysis = sales_df.groupBy("category").agg(
    spark_sum("price").alias("total_revenue"),
    spark_count("*").alias("product_count"),
    spark_sum(when(col("status") == "Active", 1).otherwise(0)).alias("active_products")
)

category_analysis.show()

# COMMAND ----------

# Status analysis
print("📊 Product Status Distribution:")
status_distribution = sales_df.groupBy("status").count()
status_distribution.show()

# COMMAND ----------

# Price range analysis
print("💰 Price Range Analysis:")
price_ranges = sales_df.withColumn("price_range", 
    when(col("price") < 100, "Under $100")
    .when(col("price") < 300, "$100-$300")
    .when(col("price") < 500, "$300-$500")
    .otherwise("Over $500")
)

price_range_analysis = price_ranges.groupBy("price_range").agg(
    spark_count("*").alias("product_count"),
    spark_sum("price").alias("total_value")
)

price_range_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Results to Your User Folder
# MAGIC 
# MAGIC Save the analysis results to your Databricks user folder.

# COMMAND ----------

# Save category analysis
category_output = "/Users/kaustav.paul@databricks.com/category_analysis.parquet"
category_analysis.write.mode("overwrite").parquet(category_output)
print(f"✅ Category analysis saved to: {category_output}")

# Save price range analysis
price_output = "/Users/kaustav.paul@databricks.com/price_analysis.parquet"
price_range_analysis.write.mode("overwrite").parquet(price_output)
print(f"✅ Price analysis saved to: {price_output}")

# Save full dataset
full_output = "/Users/kaustav.paul@databricks.com/sales_data.parquet"
sales_df.write.mode("overwrite").parquet(full_output)
print(f"✅ Full sales data saved to: {full_output}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test IDE Features
# MAGIC 
# MAGIC Try these IDE features while running this notebook:

# COMMAND ----------

# You can set breakpoints here and debug!
print("🔍 Try setting a breakpoint on this line and debugging!")

# You can use IntelliSense and autocomplete
sample_list = [1, 2, 3, 4, 5]
# Try typing: sample_list. and see autocomplete suggestions

print("✅ IDE features are working!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC 
# MAGIC **What you just accomplished:**
# MAGIC - ✅ **Connected to remote cluster** from your local IDE
# MAGIC - ✅ **Executed code locally** while using cluster resources
# MAGIC - ✅ **Used IDE features** like debugging and IntelliSense
# MAGIC - ✅ **Saved results** to your Databricks user folder
# MAGIC - ✅ **Full development experience** with remote execution
# MAGIC 
# MAGIC **Next steps:**
# MAGIC - 🚀 **Run individual cells** by clicking the play button
# MAGIC - 🔍 **Set breakpoints** and debug your code
# MAGIC - 📝 **Modify the code** and see real-time results
# MAGIC - 💾 **Create more notebooks** and develop locally

print("🎉 IDE Integration Demo Completed Successfully!")
print("🚀 You're now developing with Databricks Connect in your IDE!")
print("💡 Try running individual cells and see the magic happen!")
