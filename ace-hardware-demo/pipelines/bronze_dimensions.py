"""
Bronze Layer: Dimension Tables (Batch)
Loads reference data for stores, vendors, shipments, products, and line items
"""
import dlt
from config import (
    DIMENSIONS_PATH,
    STORES_SCHEMA,
    VENDORS_SCHEMA,
    SHIPMENTS_SCHEMA,
    PRODUCTS_SCHEMA,
    SHIPMENT_LINE_ITEMS_SCHEMA
)


@dlt.table(
    name="stores_bronze",
    comment="Store locations and attributes",
    table_properties={"quality": "bronze"}
)
def stores_bronze():
    """ACE Hardware store master data with GPS coordinates and revenue"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(STORES_SCHEMA)
        .load(f"{DIMENSIONS_PATH}/stores.csv")
    )


@dlt.table(
    name="vendors_bronze",
    comment="Vendor master data with performance metrics",
    table_properties={"quality": "bronze"}
)
def vendors_bronze():
    """Vendor information including on-time performance and risk tier"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(VENDORS_SCHEMA)
        .load(f"{DIMENSIONS_PATH}/vendors.csv")
    )


@dlt.table(
    name="shipments_bronze",
    comment="Shipment master data with origin and destination details",
    table_properties={"quality": "bronze"}
)
def shipments_bronze():
    """Shipment details including origin GPS, carrier, and planned arrival"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(SHIPMENTS_SCHEMA)
        .load(f"{DIMENSIONS_PATH}/shipments.csv")
    )


@dlt.table(
    name="products_bronze",
    comment="Product catalog with categories and pricing",
    table_properties={"quality": "bronze"}
)
def products_bronze():
    """Product master data including temperature control requirements"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(PRODUCTS_SCHEMA)
        .load(f"{DIMENSIONS_PATH}/products.csv")
    )


@dlt.table(
    name="shipment_line_items_bronze",
    comment="Shipment line-level details linking products to shipments",
    table_properties={"quality": "bronze"}
)
def shipment_line_items_bronze():
    """Detailed line items for each shipment with quantities and values"""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(SHIPMENT_LINE_ITEMS_SCHEMA)
        .load(f"{DIMENSIONS_PATH}/shipment_line_items.csv")
    )
