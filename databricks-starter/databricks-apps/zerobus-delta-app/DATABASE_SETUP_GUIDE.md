# Database Setup Guide - Zerobus Delta App

**Workspace**: `https://e2-demo-field-eng.cloud.databricks.com`  
**Workspace ID**: `1444828305810485`  
**Service Principal**: `e2037d44-6c92-4fee-9ed5-e59f70eb7107`

---

## 📋 Overview

This guide walks you through setting up the complete database infrastructure for your Zerobus Delta App, including:

1. ✅ Catalog creation/verification
2. ✅ Schema creation
3. ✅ Table creation (Zerobus-compatible)
4. ✅ Service Principal permissions
5. ✅ Testing and verification

**Total Time**: ~5-10 minutes

---

## 🚀 Quick Start (3 Options)

### **Option 1: Run Complete Setup Script** ⭐ Recommended

1. Open Databricks SQL Editor: https://e2-demo-field-eng.cloud.databricks.com/sql/editor
2. Open the file: `sql/00_complete_setup.sql`
3. Copy entire script into SQL Editor
4. Click **Run All** or execute step-by-step
5. Verify success messages

### **Option 2: Run via Databricks CLI**

```bash
# From your terminal
databricks workspace import \
  sql/00_complete_setup.sql \
  /Users/kaustav.paul@databricks.com/zerobus-setup.sql \
  --language SQL \
  --profile DEFAULT

# Then run it in a SQL warehouse via CLI
databricks sql-statements execute \
  --warehouse-id <your-warehouse-id> \
  --statement "$(cat sql/00_complete_setup.sql)" \
  --profile DEFAULT
```

### **Option 3: Run in Databricks Notebook**

1. Create new SQL notebook
2. Copy contents of `sql/00_complete_setup.sql`
3. Execute cells one by one
4. Verify each step

---

## 📊 Database Structure

```
kaustavpaul_demo (CATALOG)
└── zerobus_delta (SCHEMA)
    └── zerobus_products (TABLE) ⭐ PRIMARY
```

### **Table Schema**

Both tables have identical structure:

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | Unique record identifier (UUID) |
| `batch_id` | STRING | Batch processing identifier |
| `processed_at` | STRING | Processing timestamp (ISO 8601) |
| `source` | STRING | Data source (includes writer method) |
| `product_id` | STRING | Unique product identifier |
| `product_name` | STRING | Product name |
| `product_price` | DOUBLE | Product price in USD |
| `category` | STRING | Product category |
| `sale_start_date` | STRING | Sale start date (YYYY-MM-DD) |
| `sale_stop_date` | STRING | Sale stop date (YYYY-MM-DD) |

---

## 🔧 Step-by-Step Instructions

### **Step 1: Check Prerequisites** ✅

Before starting, verify:

```sql
-- Check current user
SELECT current_user() as current_user;

-- Check available catalogs
SHOW CATALOGS;

-- Check SQL warehouse is running
-- (Should already be connected in SQL Editor)
```

**Expected**: You should see your username and available catalogs.

---

### **Step 2: Create/Verify Catalog** 📦

```sql
-- Create catalog (if it doesn't exist)
CREATE CATALOG IF NOT EXISTS kaustavpaul_demo
COMMENT 'Demo catalog for Kaustav Paul - contains Zerobus Delta App data';

-- Verify catalog exists
SHOW CATALOGS LIKE 'kaustavpaul_demo';

-- View catalog details
DESCRIBE CATALOG kaustavpaul_demo;
```

**Expected Output**:
```
catalog_name      | owner            | comment
------------------|------------------|------------------
kaustavpaul_demo  | kaustav.paul@... | Demo catalog...
```

---

### **Step 3: Create Schema** 📁

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS kaustavpaul_demo.zerobus_delta
COMMENT 'Schema for Zerobus Direct Write integration with Delta tables';

-- Verify schema
SHOW SCHEMAS IN kaustavpaul_demo LIKE 'zerobus_delta';

-- View schema details
DESCRIBE SCHEMA EXTENDED kaustavpaul_demo.zerobus_delta;
```

**Expected Output**:
```
database_name    | comment                        | owner
-----------------|--------------------------------|------------------
zerobus_delta    | Schema for Zerobus Direct...  | kaustav.paul@...
```

---

### **Step 4: Create Tables** 📊

#### **Primary Table** (zerobus_products)

```sql
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
)
USING DELTA
COMMENT 'Primary products table for Zerobus Direct Write'
TBLPROPERTIES (
    'delta.enableRowTracking' = 'false',
    'delta.enableDeletionVectors' = 'false',
    'delta.enableChangeDataFeed' = 'false'
);
```

**Why these properties?**
- ✅ `delta.enableRowTracking = false` - Required for Zerobus compatibility
- ✅ `delta.enableDeletionVectors = false` - Prevents compatibility issues
- ✅ `delta.enableChangeDataFeed = false` - Simplifies configuration

#### **Verify Table Creation**

```sql
-- List tables
SHOW TABLES IN kaustavpaul_demo.zerobus_delta;

-- Check table details
DESCRIBE DETAIL kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Verify table properties
SHOW TBLPROPERTIES kaustavpaul_demo.zerobus_delta.zerobus_products;
```

**Expected**: Table should show `delta.enableRowTracking = false`

---

### **Step 5: Grant Service Principal Permissions** 🔐

This is **critical** for Zerobus Writer to work!

```sql
-- Service Principal: e2037d44-6c92-4fee-9ed5-e59f70eb7107

-- Grant catalog access
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- Grant schema access
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

-- Grant table access (MODIFY + SELECT)
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;
```

**Why MODIFY and SELECT?**
- `MODIFY`: Allows INSERT, UPDATE, DELETE operations
- `SELECT`: Allows reading data for verification

**⚠️ Important**: Do NOT use `ALL_PRIVILEGES` - it can cause issues with Zerobus!

#### **Verify Permissions**

```sql
-- Check catalog grants
SHOW GRANTS ON CATALOG kaustavpaul_demo;

-- Check schema grants
SHOW GRANTS ON SCHEMA kaustavpaul_demo.zerobus_delta;

-- Check table grants
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;
```

**Expected**: You should see Service Principal ID with MODIFY and SELECT permissions.

---

### **Step 6: Test Table Insert** ✅

Insert test data to verify everything works:

```sql
-- Insert test record
INSERT INTO kaustavpaul_demo.zerobus_delta.zerobus_products
VALUES (
    'test-record-001',
    'test-batch-001',
    current_timestamp(),
    'manual_test_insertion',
    'PROD-TEST-001',
    'Test Product - iPhone 15',
    999.99,
    'electronics',
    '2024-01-01',
    '2024-12-31'
);

-- Verify test record
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products
WHERE record_id = 'test-record-001';

-- Count records
SELECT COUNT(*) as record_count 
FROM kaustavpaul_demo.zerobus_delta.zerobus_products;
```

**Expected Output**:
```
record_count
------------
1
```

#### **Clean Up Test Data** (Optional)

```sql
DELETE FROM kaustavpaul_demo.zerobus_delta.zerobus_products
WHERE record_id = 'test-record-001';
```

---

## ✅ Verification Checklist

After completing setup, verify:

- [ ] **Catalog exists**: `SHOW CATALOGS LIKE 'kaustavpaul_demo'`
- [ ] **Schema exists**: `SHOW SCHEMAS IN kaustavpaul_demo`
- [ ] **Table exists**: `SHOW TABLES IN kaustavpaul_demo.zerobus_delta`
- [ ] **Table properties correct**: `delta.enableRowTracking = false`
- [ ] **Permissions granted**: Service Principal has MODIFY + SELECT
- [ ] **Test insert works**: Can insert and read data

---

## 🔍 Troubleshooting

### **Issue: "Catalog already exists"**

**Solution**: This is fine! Use `CREATE CATALOG IF NOT EXISTS` to skip if exists.

```sql
CREATE CATALOG IF NOT EXISTS kaustavpaul_demo;
```

---

### **Issue: "Schema already exists"**

**Solution**: Also fine! Use `CREATE SCHEMA IF NOT EXISTS`.

```sql
CREATE SCHEMA IF NOT EXISTS kaustavpaul_demo.zerobus_delta;
```

---

### **Issue: "Permission denied"**

**Symptoms**: 
- Cannot create catalog/schema
- Cannot grant permissions

**Solution**: Ensure you have admin privileges

```sql
-- Check your grants
SHOW GRANTS ON CATALOG kaustavpaul_demo;

-- Contact workspace admin if you need elevated permissions
```

---

### **Issue: "Table has incompatible features"**

**Symptoms**: Zerobus Writer fails with "unsupported features" error

**Solution**: Recreate table with correct properties

```sql
-- Drop incompatible table
DROP TABLE IF EXISTS kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Recreate with correct properties
-- (Use the CREATE TABLE statement from Step 4 above)
```

---

### **Issue: "Service Principal permissions not working"**

**Solution**: Verify Service Principal exists and permissions are correct

```sql
-- Check if Service Principal has permissions
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;

-- Re-grant if needed
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;
```

---

## 🎯 What's Next?

After completing database setup:

### **1. Test Locally** (Optional)

```bash
cd /Users/kaustav.paul/CursorProjects/Databricks/databricks-starter/databricks-apps/zerobus-delta-app

# Install dependencies
pip install -r requirements.txt

# Run app
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Open browser
open http://localhost:8000
```

### **2. Deploy to Databricks**

```bash
databricks apps deploy zerobus-delta-app \
  --source-code-path /Workspace/Users/kaustav.paul@databricks.com/zerobus-delta-app \
  --profile DEFAULT
```

### **3. Test All Writers**

1. **🧪 Mock Writer** - Should work immediately
2. **🏗️ Direct Delta Writer** - Test with SQL warehouse
3. **🚀 Zerobus Writer** - Test high-performance streaming

---

## 📚 SQL Scripts Reference

| Script | Purpose | Use When |
|--------|---------|----------|
| `00_complete_setup.sql` | Complete setup from scratch | ⭐ First time setup |
| `create_new_zerobus_table.sql` | Create individual table | Table recreation |
| `grant_permissions.sql` | Grant SP permissions | Permission issues |
| `check_statement_status.sql` | Verify recent operations | Debugging |

---

## 📞 Support

If you encounter issues:

1. **Check SQL Editor**: Look for error messages
2. **Verify permissions**: Run `SHOW GRANTS` commands
3. **Check table properties**: Run `SHOW TBLPROPERTIES`
4. **Review app logs**: Access `/debug/logs` endpoint
5. **Test Service Principal**: Use debug endpoints in app

---

## 🎉 Setup Complete!

Your database infrastructure is now ready for the Zerobus Delta App!

**Summary of what was created:**
- ✅ Catalog: `kaustavpaul_demo`
- ✅ Schema: `zerobus_delta`
- ✅ Table: `zerobus_products` (primary)
- ✅ Service Principal permissions configured

**Next step**: Deploy your app and start processing data! 🚀

---

**Last Updated**: December 5, 2025  
**Workspace**: e2-demo-field-eng.cloud.databricks.com

