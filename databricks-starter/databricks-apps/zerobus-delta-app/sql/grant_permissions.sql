-- Grant permissions to Service Principal for Zerobus integration
-- Based on reference conversation (lines 76-79, 183-184)
-- Service Principal Client ID: <your-service-principal-client-id>
-- Replace <your-service-principal-client-id> with your actual Service Principal Client ID

-- Grant catalog permissions
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `<your-service-principal-client-id>`;

-- Grant schema permissions  
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `<your-service-principal-client-id>`;

-- Grant table permissions (MODIFY and SELECT only - not ALL_PERMISSIONS which is buggy)
GRANT MODIFY ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;
GRANT SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<your-service-principal-client-id>`;

-- Verify permissions (optional)
SHOW GRANTS ON CATALOG kaustavpaul_demo;
SHOW GRANTS ON SCHEMA kaustavpaul_demo.zerobus_delta;
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data;
