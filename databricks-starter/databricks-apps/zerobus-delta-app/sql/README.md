# SQL Scripts

This directory contains SQL scripts for managing the Zerobus Delta table and permissions.

## Table Management Scripts

- **`simple_table_fix.sql`** - Simple script to recreate table without advanced features
- **`comprehensive_table_fix.sql`** - Comprehensive table recreation with backup
- **`create_new_zerobus_table.sql`** - Create a new clean Zerobus-compatible table
- **`create_zerobus_compatible_table.sql`** - Create table with Zerobus compatibility
- **`fix_table_properties.sql`** - Alter existing table properties for Zerobus compatibility

## Permission Management Scripts

- **`grant_permissions.sql`** - Grant necessary permissions to Service Principal

## Diagnostic Scripts

- **`check_statement_status.sql`** - Check SQL statement execution status and recent records

## Usage

Run these scripts in Databricks SQL Editor or a Databricks notebook to manage your Delta tables and permissions for the Zerobus Direct Write integration.
