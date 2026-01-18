# Databricks notebook source
# MAGIC %md
# MAGIC # Discount Tire Executive Brief Demo
# MAGIC
# MAGIC This notebook ingests mock data, registers Unity Catalog tables, and prepares
# MAGIC queries for dashboards and Genie-driven insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure catalog and schema

# COMMAND ----------

catalog_name = "kaustavpaul_demo"
schema_name = "dtc_demo"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load CSVs from Volume and write to Delta tables
# MAGIC
# MAGIC Source data lives in the volume path below.

# COMMAND ----------

base_path = "/Volumes/kaustavpaul_demo/dtc_demo/dtc_files/data"

customers_df = spark.read.csv(f"{base_path}/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv(f"{base_path}/products.csv", header=True, inferSchema=True)
sales_df = spark.read.csv(f"{base_path}/sales.csv", header=True, inferSchema=True)
inventory_df = spark.read.csv(f"{base_path}/inventory.csv", header=True, inferSchema=True)
services_df = spark.read.csv(f"{base_path}/services.csv", header=True, inferSchema=True)
stores_df = spark.read.csv(f"{base_path}/stores.csv", header=True, inferSchema=True)
promotions_df = spark.read.csv(f"{base_path}/promotions.csv", header=True, inferSchema=True)
appointments_df = spark.read.csv(f"{base_path}/appointments.csv", header=True, inferSchema=True)
surveys_df = spark.read.csv(f"{base_path}/surveys.csv", header=True, inferSchema=True)
feedback_topics_df = spark.read.csv(f"{base_path}/feedback_topics.csv", header=True, inferSchema=True)
inventory_movements_df = spark.read.csv(f"{base_path}/inventory_movements.csv", header=True, inferSchema=True)
store_kpis_df = spark.read.csv(f"{base_path}/store_kpis.csv", header=True, inferSchema=True)

customers_df.write.mode("overwrite").saveAsTable("customers")
products_df.write.mode("overwrite").saveAsTable("products")
sales_df.write.mode("overwrite").saveAsTable("sales")
inventory_df.write.mode("overwrite").saveAsTable("inventory")
services_df.write.mode("overwrite").saveAsTable("services")
stores_df.write.mode("overwrite").saveAsTable("stores")
promotions_df.write.mode("overwrite").saveAsTable("promotions")
appointments_df.write.mode("overwrite").saveAsTable("appointments")
surveys_df.write.mode("overwrite").saveAsTable("surveys")
feedback_topics_df.write.mode("overwrite").saveAsTable("feedback_topics")
inventory_movements_df.write.mode("overwrite").saveAsTable("inventory_movements")
store_kpis_df.write.mode("overwrite").saveAsTable("store_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create metric-ready views
# MAGIC
# MAGIC If your workspace supports Unity Catalog Metric Views, replace these with
# MAGIC `CREATE METRIC VIEW` statements.

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE VIEW vw_sales_enriched AS
    SELECT
      s.sale_id,
      s.date,
      s.customer_id,
      s.product_id,
      s.quantity,
      s.unit_price,
      s.total_amount,
      s.store_id,
      p.category,
      p.product_name,
      c.region AS customer_region,
      c.satisfaction_score,
      st.store_name,
      st.region AS store_region,
      st.state,
      pr.promo_name,
      pr.discount_type,
      pr.discount_value
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN stores st ON s.store_id = st.store_id
    LEFT JOIN promotions pr ON s.promotion_id = pr.promo_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE VIEW vw_revenue_growth AS
    SELECT
      date_trunc('month', date) AS month,
      SUM(total_amount) AS revenue,
      LAG(SUM(total_amount)) OVER (ORDER BY date_trunc('month', date)) AS prior_revenue,
      CASE
        WHEN LAG(SUM(total_amount)) OVER (ORDER BY date_trunc('month', date)) IS NULL THEN NULL
        WHEN LAG(SUM(total_amount)) OVER (ORDER BY date_trunc('month', date)) = 0 THEN NULL
        ELSE
          (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY date_trunc('month', date)))
          / LAG(SUM(total_amount)) OVER (ORDER BY date_trunc('month', date))
      END AS revenue_growth
    FROM sales
    GROUP BY date_trunc('month', date)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Dashboard query building blocks

# COMMAND ----------

# Top selling tires by revenue
top_tires = spark.sql(
    """
    SELECT
      product_name,
      SUM(total_amount) AS revenue,
      SUM(quantity) AS units_sold
    FROM vw_sales_enriched
    WHERE category = 'Tire'
    GROUP BY product_name
    ORDER BY revenue DESC
    LIMIT 10
    """
)
display(top_tires)

# Satisfaction trends by region
satisfaction_by_region = spark.sql(
    """
    SELECT
      region,
      AVG(satisfaction_score) AS avg_satisfaction
    FROM customers
    GROUP BY region
    ORDER BY avg_satisfaction DESC
    """
)
display(satisfaction_by_region)

# Inventory nearing reorder threshold
inventory_alerts = spark.sql(
    """
    SELECT
      i.store_id,
      p.product_name,
      i.stock_qty,
      i.reorder_threshold
    FROM inventory i
    JOIN products p ON i.product_id = p.product_id
    WHERE i.stock_qty <= i.reorder_threshold
    ORDER BY i.stock_qty ASC
    """
)
display(inventory_alerts)

# COMMAND ----------

# Appointment volume and no-show rate
appointment_metrics = spark.sql(
    """
    SELECT
      date_trunc('month', appointment_date) AS month,
      COUNT(*) AS total_appointments,
      SUM(CASE WHEN status = 'NoShow' THEN 1 ELSE 0 END) AS no_shows
    FROM appointments
    GROUP BY date_trunc('month', appointment_date)
    ORDER BY month
    """
)
display(appointment_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Genie and voice workflow (outline)
# MAGIC
# MAGIC - Configure Genie to use the `vw_sales_enriched` and `vw_revenue_growth` views.
# MAGIC - Ask: "What was revenue growth last quarter?" and validate the SQL.
# MAGIC - Add speech-to-text and text-to-speech in the app layer.
