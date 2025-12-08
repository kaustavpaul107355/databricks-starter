# SQL Scripts

This directory contains SQL scripts for managing the Zerobus Delta table and permissions.

---

## 🚀 Quick Start

### **New Setup** ⭐ Recommended

Use the complete setup script for first-time setup:

1. **`00_complete_setup.sql`** - Complete database setup from scratch
   - Creates catalog, schema, and tables
   - Grants Service Principal permissions
   - Includes verification steps
   - **📚 See**: `DATABASE_SETUP_GUIDE.md` for detailed instructions

2. **`QUICK_REFERENCE.md`** - Quick reference card for essential commands

---

## 📊 Table Management Scripts

- **`simple_table_fix.sql`** - Simple script to recreate table without advanced features
- **`comprehensive_table_fix.sql`** - Comprehensive table recreation with backup
- **`create_new_zerobus_table.sql`** - Create a new clean Zerobus-compatible table
- **`create_zerobus_compatible_table.sql`** - Create table with Zerobus compatibility
- **`fix_table_properties.sql`** - Alter existing table properties for Zerobus compatibility

## 🔐 Permission Management Scripts

- **`grant_permissions.sql`** - Grant necessary permissions to Service Principal

## 🔍 Diagnostic Scripts

- **`check_statement_status.sql`** - Check SQL statement execution status and recent records

---

## 📚 Documentation

- **`../DATABASE_SETUP_GUIDE.md`** - Comprehensive setup guide with step-by-step instructions
- **`QUICK_REFERENCE.md`** - Quick reference for common commands

---

## 🎯 Usage

### Option 1: SQL Editor (Recommended)

1. Open SQL Editor: https://e2-demo-field-eng.cloud.databricks.com/sql/editor
2. Copy script contents
3. Execute in SQL Editor
4. Verify results

### Option 2: Databricks Notebook

1. Create new SQL notebook
2. Copy script contents into cells
3. Execute cells one by one
4. Verify each step

### Option 3: Databricks CLI

```bash
databricks sql-statements execute \
  --warehouse-id <your-warehouse-id> \
  --statement "$(cat sql/00_complete_setup.sql)" \
  --profile DEFAULT
```

---

## ✅ Database Structure

```
kaustavpaul_demo (CATALOG)
└── zerobus_delta (SCHEMA)
    ├── zerobus_products (TABLE) ⭐ PRIMARY
    └── zerobus_products_data (TABLE)   🔄 ALTERNATIVE
```

**Service Principal**: `e2037d44-6c92-4fee-9ed5-e59f70eb7107`

---

## 🆘 Need Help?

- **Complete Guide**: See `../DATABASE_SETUP_GUIDE.md`
- **Quick Commands**: See `QUICK_REFERENCE.md`
- **Troubleshooting**: See DATABASE_SETUP_GUIDE.md → Troubleshooting section
