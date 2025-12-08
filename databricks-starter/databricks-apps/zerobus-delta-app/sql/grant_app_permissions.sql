-- ============================================================================
-- GRANT PERMISSIONS TO DATABRICKS APP SERVICE PRINCIPAL
-- ============================================================================
-- This script grants the necessary permissions to the app's auto-created
-- Service Principal for Zerobus Direct Write to work.
--
-- App Service Principal: app-40zbx9 zerobus-delta-app
-- Service Principal ID: c5549a60-6255-4827-9ead-f055c0290073
--
-- Run this in Databricks SQL Editor after deploying your app
-- ============================================================================

-- Grant CATALOG permissions
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo 
TO `c5549a60-6255-4827-9ead-f055c0290073`;

-- Grant SCHEMA permissions
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta 
TO `c5549a60-6255-4827-9ead-f055c0290073`;

-- Grant TABLE permissions (MODIFY + SELECT)
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `c5549a60-6255-4827-9ead-f055c0290073`;

-- ============================================================================
-- VERIFY PERMISSIONS
-- ============================================================================

-- Check catalog grants
SHOW GRANTS ON CATALOG kaustavpaul_demo;

-- Check schema grants
SHOW GRANTS ON SCHEMA kaustavpaul_demo.zerobus_delta;

-- Check table grants
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- You should see the Service Principal "c5549a60-6255-4827-9ead-f055c0290073"
-- with the following permissions:
-- - USE_CATALOG on catalog
-- - USE_SCHEMA on schema
-- - MODIFY, SELECT on table
-- ============================================================================

