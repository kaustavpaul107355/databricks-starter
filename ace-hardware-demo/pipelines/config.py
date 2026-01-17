"""
Configuration module for ACE Hardware Logistics DLT Pipeline
Centralizes all paths, schemas, and constants
"""
import os
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Catalog and Schema Configuration
CATALOG = "kaustavpaul_demo"
SCHEMA = "ace_demo"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/ace_files"

# Data Paths
TELEMETRY_PATH = os.getenv("TELEMETRY_PATH", f"{VOLUME_BASE}/data/telemetry/")
DIMENSIONS_PATH = os.getenv("DIMENSIONS_PATH", f"{VOLUME_BASE}/data/dimensions")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", f"{VOLUME_BASE}/checkpoints")

# Streaming Configuration
TELEMETRY_CHECKPOINT = f"{CHECKPOINT_PATH}/logistics_bronze/"

# Table Schemas
LOGISTICS_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("truck_id", StringType(), nullable=False),
    StructField("shipment_id", StringType(), nullable=False),
    StructField("store_id", IntegerType(), nullable=True),
    StructField("region_id", StringType(), nullable=False),
    StructField("vendor_type", StringType(), nullable=False),
    StructField("vendor_id", StringType(), nullable=True),
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("estimated_arrival_ts", TimestampType(), nullable=False),
    StructField("actual_arrival_ts", TimestampType(), nullable=True),
    StructField("shipment_status", StringType(), nullable=False),
    StructField("delay_minutes", IntegerType(), nullable=True),
    StructField("ingest_date", DateType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("delay_reason", StringType(), nullable=True),
    StructField("carrier", StringType(), nullable=False),
    StructField("temperature_celsius", DoubleType(), nullable=True),
    StructField("shipment_value", DoubleType(), nullable=False),
])

STORES_SCHEMA = StructType([
    StructField("store_id", IntegerType(), nullable=False),
    StructField("store_name", StringType(), nullable=False),
    StructField("region_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("open_date", StringType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("weekly_revenue", DoubleType(), nullable=False),
])

VENDORS_SCHEMA = StructType([
    StructField("vendor_id", StringType(), nullable=False),
    StructField("vendor_name", StringType(), nullable=False),
    StructField("vendor_type", StringType(), nullable=False),
    StructField("risk_tier", StringType(), nullable=False),
    StructField("on_time_pct", DoubleType(), nullable=False),
])

SHIPMENTS_SCHEMA = StructType([
    StructField("shipment_id", StringType(), nullable=False),
    StructField("vendor_id", StringType(), nullable=False),
    StructField("store_id", IntegerType(), nullable=False),
    StructField("origin_city", StringType(), nullable=False),
    StructField("origin_state", StringType(), nullable=False),
    StructField("origin_latitude", DoubleType(), nullable=False),
    StructField("origin_longitude", DoubleType(), nullable=False),
    StructField("planned_departure_ts", TimestampType(), nullable=False),
    StructField("planned_arrival_ts", TimestampType(), nullable=False),
    StructField("asn_status", StringType(), nullable=False),
    StructField("carrier", StringType(), nullable=False),
    StructField("total_value", DoubleType(), nullable=False),
])

PRODUCTS_SCHEMA = StructType([
    StructField("sku", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("requires_temp_control", BooleanType(), nullable=False),
])

SHIPMENT_LINE_ITEMS_SCHEMA = StructType([
    StructField("shipment_id", StringType(), nullable=False),
    StructField("line_number", IntegerType(), nullable=False),
    StructField("sku", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("line_total", DoubleType(), nullable=False),
])
