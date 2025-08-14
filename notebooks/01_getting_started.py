# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started with Databricks
# MAGIC 
# MAGIC This notebook provides a basic introduction to working with Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration
# MAGIC 
# MAGIC First, let's set up our environment and import necessary libraries.

# COMMAND ----------

# Import common libraries
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Sample DataFrame
# MAGIC 
# MAGIC Let's create some sample data to work with.

# COMMAND ----------

# Create sample data
data = [
    ("Alice", 25, "Engineer"),
    ("Bob", 30, "Data Scientist"),
    ("Charlie", 35, "Manager"),
    ("Diana", 28, "Engineer"),
    ("Eve", 32, "Data Scientist")
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "age", "role"])

# Display the data
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Data Operations
# MAGIC 
# MAGIC Let's perform some basic operations on our data.

# COMMAND ----------

# Show schema
df.printSchema()

# COMMAND ----------

# Basic filtering
engineers = df.filter(df.role == "Engineer")
display(engineers)

# COMMAND ----------

# Aggregations
role_counts = df.groupBy("role").count()
display(role_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Delta Lake
# MAGIC 
# MAGIC Let's save our data as a Delta table.

# COMMAND ----------

# Save as Delta table
df.write.mode("overwrite").saveAsTable("sample_employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success!
# MAGIC 
# MAGIC You've successfully run your first Databricks notebook! 
# MAGIC 
# MAGIC Next steps:
# MAGIC - Explore more data operations
# MAGIC - Connect to external data sources
# MAGIC - Build machine learning models
# MAGIC - Create data pipelines
