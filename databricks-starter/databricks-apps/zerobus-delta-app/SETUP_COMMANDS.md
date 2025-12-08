# Quick Setup Commands

**Table Name**: `kaustavpaul_demo.zerobus_delta.zerobus_products`

---

## 🚀 Option 1: Run Complete Script (Recommended)

1. Open SQL Editor: https://e2-demo-field-eng.cloud.databricks.com/sql/editor
2. Copy and run entire file: `sql/00_complete_setup.sql`
3. Done! ✅

---

## ⚡ Option 2: Quick Copy-Paste

Copy and paste this into SQL Editor:

```sql
-- 1. Create catalog
CREATE CATALOG IF NOT EXISTS kaustavpaul_demo;

-- 2. Create schema
CREATE SCHEMA IF NOT EXISTS kaustavpaul_demo.zerobus_delta;

-- 3. Create table
CREATE TABLE IF NOT EXISTS kaustavpaul_demo.zerobus_delta.zerobus_products (
    record_id STRING,
    batch_id STRING,
    processed_at STRING,
    source STRING,
    product_id STRING,
    product_name STRING,
    product_price DOUBLE,
    category STRING,
    sale_start_date STRING,
    sale_stop_date STRING
) USING DELTA
COMMENT 'Primary products table for Zerobus Direct Write'
TBLPROPERTIES (
    'delta.enableRowTracking' = 'false',
    'delta.enableDeletionVectors' = 'false',
    'delta.enableChangeDataFeed' = 'false'
);

-- 4. Grant permissions to Service Principal
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- 5. Verify setup
SHOW TABLES IN kaustavpaul_demo.zerobus_delta;
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;
```

---

## ✅ Verification

After running setup, verify with:

```sql
-- Should show: zerobus_products
SHOW TABLES IN kaustavpaul_demo.zerobus_delta;

-- Should show Service Principal with MODIFY + SELECT
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Should show: delta.enableRowTracking = false
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products;
```

---

## 🎯 Expected Results

```
✅ Catalog: kaustavpaul_demo
✅ Schema: zerobus_delta  
✅ Table: zerobus_products
✅ Permissions: Service Principal has MODIFY + SELECT
✅ Properties: delta.enableRowTracking = false
```

---

## 🚀 Deploy App

After database setup:

```bash
databricks apps deploy zerobus-delta-app \
  --source-code-path /Workspace/Users/kaustav.paul@databricks.com/zerobus-delta-app \
  --profile DEFAULT
```

---

**Time to Complete**: ~2 minutes ⏱️


