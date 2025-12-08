-- ============================================================================
-- COMPLETE DATABASE SETUP FOR ZEROBUS DELTA APP
-- ============================================================================
-- This script sets up everything needed for the Zerobus Delta App:
-- 1. Catalog (or verify it exists)
-- 2. Schema creation
-- 3. Table creation (Zerobus-compatible)
-- 4. Service Principal permissions
-- 
-- Workspace: https://e2-demo-field-eng.cloud.databricks.com
-- Workspace ID: 1444828305810485
-- Service Principal: e2037d44-6c92-4fee-9ed5-e59f70eb7107
-- 
-- Run this script in Databricks SQL Editor or a SQL notebook
-- ============================================================================

-- ============================================================================
-- STEP 1: CATALOG SETUP
-- ============================================================================

-- Option 1: Create a new catalog (if it doesn't exist)
-- Uncomment the line below if you want to create a new catalog
-- CREATE CATALOG IF NOT EXISTS kaustavpaul_demo;

-- Option 2: Verify existing catalog
-- Check if catalog exists
SHOW CATALOGS LIKE 'kaustavpaul_demo';

-- View catalog details
DESCRIBE CATALOG kaustavpaul_demo;

-- If catalog doesn't exist, create it:
CREATE CATALOG IF NOT EXISTS kaustavpaul_demo
COMMENT 'Demo catalog for Kaustav Paul - contains Zerobus Delta App data';

-- ============================================================================
-- STEP 2: SCHEMA SETUP
-- ============================================================================

-- Create schema for Zerobus data
CREATE SCHEMA IF NOT EXISTS kaustavpaul_demo.zerobus_delta
COMMENT 'Schema for Zerobus Direct Write integration with Delta tables';

-- Verify schema was created
SHOW SCHEMAS IN kaustavpaul_demo LIKE 'zerobus_delta';

-- View schema details
DESCRIBE SCHEMA EXTENDED kaustavpaul_demo.zerobus_delta;

-- ============================================================================
-- STEP 3: TABLE CREATION
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: Primary Products Table (Zerobus-compatible)
-- ----------------------------------------------------------------------------
-- This is the main table used by the application
-- IMPORTANT: No advanced Delta features to ensure Zerobus compatibility

CREATE TABLE IF NOT EXISTS kaustavpaul_demo.zerobus_delta.zerobus_products (
    -- Record metadata
    record_id STRING COMMENT 'Unique record identifier (UUID)',
    batch_id STRING COMMENT 'Batch processing identifier',
    processed_at STRING COMMENT 'Processing timestamp (ISO format)',
    source STRING COMMENT 'Data source identifier (includes writer method)',
    
    -- Product information
    product_id STRING COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Product name',
    product_price DOUBLE COMMENT 'Product price in USD',
    category STRING COMMENT 'Product category',
    
    -- Sale information
    sale_start_date STRING COMMENT 'Sale start date (YYYY-MM-DD)',
    sale_stop_date STRING COMMENT 'Sale stop date (YYYY-MM-DD)'
)
USING DELTA
COMMENT 'Primary products table for Zerobus Direct Write - clean configuration'
TBLPROPERTIES (
    -- Disable advanced features for Zerobus compatibility
    'delta.enableRowTracking' = 'false',
    'delta.enableDeletionVectors' = 'false',
    'delta.enableChangeDataFeed' = 'false'
);

-- Note: Only one table (zerobus_products) is created as the primary table
-- for all data writes from the application.

-- ============================================================================
-- STEP 4: VERIFY TABLE CREATION
-- ============================================================================

-- List all tables in the schema
SHOW TABLES IN kaustavpaul_demo.zerobus_delta;

-- Check table details
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Verify table properties (ensure no incompatible features)
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Check table schema
DESCRIBE TABLE EXTENDED kaustavpaul_demo.zerobus_delta.zerobus_products;

-- ============================================================================
-- STEP 5: SERVICE PRINCIPAL PERMISSIONS
-- ============================================================================
-- Grant permissions to Service Principal for Zerobus Direct Write
-- Service Principal Application ID: e2037d44-6c92-4fee-9ed5-e59f70eb7107

-- Grant CATALOG permissions
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- Grant SCHEMA permissions
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- Grant TABLE permissions
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- ============================================================================
-- STEP 6: VERIFY PERMISSIONS
-- ============================================================================

-- View all grants on catalog
SHOW GRANTS ON CATALOG kaustavpaul_demo;

-- View all grants on schema
SHOW GRANTS ON SCHEMA kaustavpaul_demo.zerobus_delta;

-- View all grants on table
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;

-- ============================================================================
-- STEP 7: TEST DATA INSERTION (Optional)
-- ============================================================================
-- Insert test data to verify everything works

-- Insert a test record
INSERT INTO kaustavpaul_demo.zerobus_delta.zerobus_products
VALUES (
    'test-record-001',                    -- record_id
    'test-batch-001',                     -- batch_id
    current_timestamp(),                  -- processed_at
    'manual_test_insertion',              -- source
    'PROD-TEST-001',                      -- product_id
    'Test Product - iPhone 15',           -- product_name
    999.99,                               -- product_price
    'electronics',                        -- category
    '2024-01-01',                         -- sale_start_date
    '2024-12-31'                          -- sale_stop_date
);

-- Verify test record was inserted
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products
WHERE record_id = 'test-record-001';

-- Count records in table
SELECT COUNT(*) as record_count 
FROM kaustavpaul_demo.zerobus_delta.zerobus_products;

-- ============================================================================
-- STEP 8: CLEANUP TEST DATA (Optional)
-- ============================================================================
-- Remove test data after verification

-- DELETE FROM kaustavpaul_demo.zerobus_delta.zerobus_products
-- WHERE record_id = 'test-record-001';

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================
-- ✅ Catalog: kaustavpaul_demo
-- ✅ Schema: zerobus_delta
-- ✅ Table: zerobus_products
-- ✅ Service Principal Permissions: Granted
-- 
-- Your Zerobus Delta App is now ready to use!
-- 
-- Next steps:
-- 1. Deploy your app: databricks apps deploy zerobus-delta-app
-- 2. Test the web UI: Access your deployed app URL
-- 3. Test data writes: Use the web interface to submit products
-- ============================================================================

