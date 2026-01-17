"""
Gold Layer: Business Metrics for FLO (Fulfillment & Logistics Optimization)
Aggregated tables for analytics and dashboards
"""
import dlt
from pyspark.sql.functions import avg, col, count, sum as _sum, max as _max


@dlt.table(
    name="store_delay_metrics",
    comment="Store-level delay analysis for stockout risk assessment",
    table_properties={"quality": "gold"}
)
def store_delay_metrics():
    """
    Aggregates delivery performance by store for FLO risk modeling.
    Supports: Stockout prediction, store performance dashboards
    """
    delivered = dlt.read("logistics_silver").filter(col("event_type") == "DELIVERED")
    
    return (
        delivered
        .groupBy(
            "store_id", "store_name", "store_city", "store_state",
            "region_id", "store_latitude", "store_longitude", "store_weekly_revenue"
        )
        .agg(
            count("*").alias("total_deliveries"),
            _sum("delay_minutes").alias("total_delay_minutes"),
            avg("delay_minutes").alias("avg_delay_minutes"),
            _max("delay_minutes").alias("max_delay_minutes"),
            count("delay_minutes").alias("delayed_shipments"),
            _sum("shipment_value").alias("total_shipment_value"),
            avg("temperature_celsius").alias("avg_temperature")
        )
    )


@dlt.table(
    name="vendor_performance",
    comment="Vendor on-time performance and delivery metrics by region",
    table_properties={"quality": "gold"}
)
def vendor_performance():
    """
    Vendor scorecarding for supplier management.
    Supports: Vendor selection, contract negotiation, risk mitigation
    """
    delivered = dlt.read("logistics_silver").filter(col("event_type") == "DELIVERED")
    
    return (
        delivered
        .groupBy("vendor_id", "vendor_name", "vendor_type", "vendor_risk_tier", "region_id")
        .agg(
            count("*").alias("total_deliveries"),
            count("delay_minutes").alias("delayed_deliveries"),
            avg("delay_minutes").alias("avg_delay_minutes"),
            _sum("shipment_value").alias("total_value_delivered")
        )
    )


@dlt.table(
    name="carrier_performance",
    comment="Carrier comparison metrics for logistics optimization",
    table_properties={"quality": "gold"}
)
def carrier_performance():
    """
    Carrier benchmarking for route optimization and contract decisions.
    Supports: Carrier selection, cost optimization, SLA monitoring
    """
    delivered = dlt.read("logistics_silver").filter(col("event_type") == "DELIVERED")
    
    return (
        delivered
        .groupBy("carrier")
        .agg(
            count("*").alias("total_deliveries"),
            count("delay_minutes").alias("delayed_deliveries"),
            avg("delay_minutes").alias("avg_delay_minutes"),
            _max("delay_minutes").alias("max_delay_minutes"),
            _sum("shipment_value").alias("total_value_delivered")
        )
    )


@dlt.table(
    name="product_category_metrics",
    comment="Product category delivery analysis with temperature monitoring",
    table_properties={"quality": "gold"}
)
def product_category_metrics():
    """
    Product-level logistics performance for inventory planning.
    Supports: Category-specific delivery SLAs, temp-sensitive goods tracking
    """
    # Join line items with products for category analysis
    line_items = dlt.read("shipment_line_items_bronze").alias("li")
    products = dlt.read("products_bronze").alias("p")
    
    enriched_items = line_items.join(
        products,
        col("li.sku") == col("p.sku"),
        "left"
    ).select(
        "li.shipment_id", "li.quantity", "li.line_total",
        "p.category", "p.requires_temp_control"
    )
    
    # Join with delivered telemetry
    delivered = dlt.read("logistics_silver").filter(col("event_type") == "DELIVERED")
    
    product_delivery = enriched_items.alias("items").join(
        delivered.alias("d"),
        col("items.shipment_id") == col("d.shipment_id"),
        "inner"
    )
    
    return (
        product_delivery
        .groupBy("category", "region_id", "requires_temp_control")
        .agg(
            _sum("quantity").alias("total_units_shipped"),
            _sum("line_total").alias("total_value_shipped"),
            count("*").alias("line_items_delivered"),
            avg("d.delay_minutes").alias("avg_delivery_delay"),
            avg("d.temperature_celsius").alias("avg_temperature")
        )
    )
