-- Fix Table Properties for Zerobus Compatibility
-- Based on reference implementation requirements

-- Update table properties to disable problematic features
ALTER TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableRowTracking' = 'false'  -- This is crucial for Zerobus compatibility
);

-- Verify the table properties
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products_data;

-- Check table details
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;
