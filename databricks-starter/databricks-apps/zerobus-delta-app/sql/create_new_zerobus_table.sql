-- Create a brand new Zerobus-compatible table
-- This avoids any issues with the existing table

-- Create a new table with a clean name
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
    -- Minimal properties - no advanced features
    'delta.enableRowTracking' = 'false'
);

-- Grant permissions to Service Principal
-- Replace <your-service-principal-client-id> with your actual Service Principal Client ID
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `<your-service-principal-client-id>`;
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `<your-service-principal-client-id>`;
GRANT MODIFY ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;
GRANT SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;

-- Verify the table is clean
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products_data;
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products_data;
