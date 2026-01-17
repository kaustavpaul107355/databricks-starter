"""
Silver Layer: Enriched Logistics Telemetry
Joins telemetry with dimensions and applies data quality rules
"""
import dlt
from pyspark.sql.functions import coalesce, col


# Data Quality Expectations
QUALITY_RULES = {
    "store_id_not_null": "store_id IS NOT NULL",
    "vendor_id_not_null": "vendor_id IS NOT NULL",
    "event_ts_not_null": "event_ts IS NOT NULL",
    "valid_status": "shipment_status IN ('ON_TIME','DELAYED','IN_TRANSIT','PENDING')",
    "valid_vendor_type": "vendor_type IN ('ACE','NON_ACE')",
    "valid_event_type": "event_type IN ('SHIPMENT_CREATED','DEPARTED_WAREHOUSE','IN_TRANSIT','ARRIVED_DC','OUT_FOR_DELIVERY','DELIVERED','EXCEPTION')"
}


@dlt.table(
    name="logistics_silver",
    comment="Enriched logistics telemetry with store, vendor, and shipment context",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true"
    }
)
@dlt.expect_or_drop("store_id_not_null", QUALITY_RULES["store_id_not_null"])
@dlt.expect_or_drop("vendor_id_not_null", QUALITY_RULES["vendor_id_not_null"])
@dlt.expect_or_drop("event_ts_not_null", QUALITY_RULES["event_ts_not_null"])
@dlt.expect_or_drop("valid_status", QUALITY_RULES["valid_status"])
@dlt.expect_or_drop("valid_vendor_type", QUALITY_RULES["valid_vendor_type"])
@dlt.expect_or_drop("valid_event_type", QUALITY_RULES["valid_event_type"])
def logistics_silver():
    """
    Enriched telemetry with full context:
    - Store details (location, revenue, status)
    - Vendor performance metrics
    - Shipment details (origin, carrier, value)
    - Data quality enforced via expectations
    """
    # Read source tables
    telemetry = dlt.read("logistics_bronze").alias("t")
    shipments = dlt.read("shipments_bronze").alias("s")
    stores = dlt.read("stores_bronze").alias("st")
    vendors = dlt.read("vendors_bronze").alias("v")

    # Enrich telemetry with shipment details
    enriched = telemetry.join(
        shipments,
        col("t.shipment_id") == col("s.shipment_id"),
        "left"
    ).select(
        # Core telemetry fields
        col("t.event_id"),
        col("t.truck_id"),
        col("t.shipment_id"),
        col("t.store_id"),
        col("t.region_id"),
        coalesce(col("t.vendor_id"), col("s.vendor_id")).alias("vendor_id"),
        col("t.vendor_type"),
        col("t.event_ts"),
        col("t.latitude"),
        col("t.longitude"),
        col("t.estimated_arrival_ts"),
        col("t.actual_arrival_ts"),
        col("t.shipment_status"),
        col("t.delay_minutes"),
        col("t.ingest_date"),
        col("t.event_type"),
        col("t.delay_reason"),
        col("t.carrier"),
        col("t.temperature_celsius"),
        col("t.shipment_value"),
        # Shipment context
        col("s.origin_city"),
        col("s.origin_state"),
        col("s.origin_latitude"),
        col("s.origin_longitude"),
        col("s.planned_departure_ts"),
        col("s.planned_arrival_ts"),
        col("s.asn_status"),
        col("s.total_value").alias("shipment_total_value")
    )

    # Add store context
    enriched = enriched.alias("e").join(
        stores,
        col("e.store_id") == col("st.store_id"),
        "left"
    ).select(
        "e.*",
        col("st.store_name"),
        col("st.city").alias("store_city"),
        col("st.state").alias("store_state"),
        col("st.latitude").alias("store_latitude"),
        col("st.longitude").alias("store_longitude"),
        col("st.open_date").alias("store_open_date"),
        col("st.is_active").alias("store_is_active"),
        col("st.weekly_revenue").alias("store_weekly_revenue")
    )

    # Add vendor context
    return enriched.alias("e2").join(
        vendors,
        col("e2.vendor_id") == col("v.vendor_id"),
        "left"
    ).select(
        "e2.*",
        col("v.vendor_name"),
        col("v.risk_tier").alias("vendor_risk_tier"),
        col("v.on_time_pct").alias("vendor_on_time_pct")
    )
