-- Create Zerobus-Compatible Delta Table
-- This script recreates the products table without advanced Delta features that Zerobus doesn't support

-- Step 1: Backup existing data (if any)
CREATE OR REPLACE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data_backup AS 
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 2: Drop the existing table with incompatible features
DROP TABLE IF EXISTS kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 3: Create new Zerobus-compatible table
CREATE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data (
    record_id STRING COMMENT 'Unique record identifier',
    product_id STRING COMMENT 'Product identifier', 
    product_name STRING COMMENT 'Product name',
    product_price DOUBLE COMMENT 'Product price in USD',
    category STRING COMMENT 'Product category',
    sale_start_date STRING COMMENT 'Sale start date (YYYY-MM-DD)',
    sale_stop_date STRING COMMENT 'Sale stop date (YYYY-MM-DD)', 
    processed_at STRING COMMENT 'Processing timestamp (ISO 8601)',
    batch_id STRING COMMENT 'Processing batch identifier',
    source STRING COMMENT 'Data source identifier'
) 
USING DELTA
LOCATION 's3://your-bucket/kaustavpaul_demo/zerobus_delta/zerobus_products_data/'
COMMENT 'Product data table compatible with Zerobus Direct Write API'
TBLPROPERTIES (
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.feature.timestampNtz' = 'supported'
);

-- Step 4: Restore data from backup (if needed)
-- INSERT INTO kaustavpaul_demo.zerobus_delta.zerobus_products_data 
-- SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data_backup;

-- Step 5: Verify table properties
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 6: Show supported features
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products_data;
