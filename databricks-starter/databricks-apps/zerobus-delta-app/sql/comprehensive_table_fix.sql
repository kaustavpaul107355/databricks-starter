-- Comprehensive Zerobus Table Fix
-- This script ensures the table is completely clean of unsupported features

-- Step 1: Check current table properties
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 2: Backup existing data (if any)
CREATE OR REPLACE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data_backup AS 
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 3: Drop the table completely
DROP TABLE IF EXISTS kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 4: Recreate with minimal, Zerobus-compatible configuration
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
) USING DELTA
TBLPROPERTIES (
    -- Explicitly disable all unsupported features
    'delta.enableRowTracking' = 'false',
    'delta.feature.domainMetadata' = 'disabled',
    -- Only enable basic supported features
    'delta.autoOptimize.optimizeWrite' = 'false',
    'delta.autoOptimize.autoCompact' = 'false'
);

-- Step 5: Verify the table is clean
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 6: Check table properties
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Step 7: Grant permissions again to the Service Principal
-- Replace <your-service-principal-client-id> with your actual Service Principal Client ID
GRANT MODIFY ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;
GRANT SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;

-- Step 8: Test insert to verify table works
INSERT INTO kaustavpaul_demo.zerobus_delta.zerobus_products_data VALUES 
('test-record-id', 'TEST001', 'Test Product', 99.99, 'test', '2024-01-01', '2024-12-31', '2024-01-01T00:00:00', 'test-batch', 'test');

-- Step 9: Verify the test record
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data WHERE product_id = 'TEST001';

-- Step 10: Clean up test record
DELETE FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data WHERE product_id = 'TEST001';
