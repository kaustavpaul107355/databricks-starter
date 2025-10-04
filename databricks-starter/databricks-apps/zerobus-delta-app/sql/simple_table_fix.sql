-- Simple Zerobus Table Fix
-- Run these commands in Databricks SQL Editor or Notebook

-- 1. Backup existing data
CREATE OR REPLACE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data_backup AS 
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- 2. Drop and recreate without advanced features
DROP TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data;

CREATE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data (
    record_id STRING,
    product_id STRING,
    product_name STRING,
    product_price DOUBLE,
    category STRING,
    sale_start_date STRING,
    sale_stop_date STRING,
    processed_at STRING,
    batch_id STRING,
    source STRING
) USING DELTA;

-- 3. Verify it's clean (should show no advanced features)
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;
