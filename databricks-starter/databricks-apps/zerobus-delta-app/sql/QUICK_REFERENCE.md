# Database Setup - Quick Reference

## 🚀 One-Command Setup

Open **SQL Editor** and run:

```sql
-- Run the complete setup script
-- File: sql/00_complete_setup.sql
-- Time: ~2 minutes
```

---

## 📊 Database Structure

```
kaustavpaul_demo.zerobus_delta.zerobus_products
```

**Catalog**: `kaustavpaul_demo`  
**Schema**: `zerobus_delta`  
**Table**: `zerobus_products` (primary)

---

## 🔑 Service Principal

**Application ID**: `e2037d44-6c92-4fee-9ed5-e59f70eb7107`

**Permissions Needed**:
- ✅ USE_CATALOG on catalog
- ✅ USE_SCHEMA on schema  
- ✅ MODIFY + SELECT on table

---

## ✅ Quick Verification

```sql
-- 1. Check catalog
SHOW CATALOGS LIKE 'kaustavpaul_demo';

-- 2. Check schema
SHOW SCHEMAS IN kaustavpaul_demo LIKE 'zerobus_delta';

-- 3. Check table
SHOW TABLES IN kaustavpaul_demo.zerobus_delta;

-- 4. Check permissions
SHOW GRANTS ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products;

-- 5. Test insert
SELECT COUNT(*) FROM kaustavpaul_demo.zerobus_delta.zerobus_products;
```

---

## 🔧 Common Commands

### Create Everything

```sql
-- Catalog
CREATE CATALOG IF NOT EXISTS kaustavpaul_demo;

-- Schema
CREATE SCHEMA IF NOT EXISTS kaustavpaul_demo.zerobus_delta;

-- Table
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
TBLPROPERTIES ('delta.enableRowTracking' = 'false');
```

### Grant Permissions

```sql
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;

GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products 
TO `e2037d44-6c92-4fee-9ed5-e59f70eb7107`;
```

---

## 🎯 Access URLs

**SQL Editor**: https://e2-demo-field-eng.cloud.databricks.com/sql/editor  
**Workspace**: https://e2-demo-field-eng.cloud.databricks.com

---

## 📄 Complete Documentation

See `DATABASE_SETUP_GUIDE.md` for detailed instructions.

