-- Check the status of the pending SQL statement
-- Statement ID: 01f0a090-094d-15cf-b840-8373cb61fdcb

-- Check if the record was eventually written
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_clean 
WHERE batch_id = '996114b1-f8af-4bf9-9d23-9bff07cf5061'
ORDER BY processed_at DESC;

-- Check recent records to see if any were written
SELECT 
    source,
    batch_id,
    processed_at,
    product_id,
    product_name
FROM kaustavpaul_demo.zerobus_delta.zerobus_products_clean 
ORDER BY processed_at DESC 
LIMIT 10;
