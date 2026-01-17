"""
Bronze Layer: Streaming Telemetry Ingestion
Auto Loader ingests logistics telemetry CSV files from Volume storage
"""
import dlt
from config import LOGISTICS_SCHEMA, TELEMETRY_PATH, TELEMETRY_CHECKPOINT


@dlt.table(
    name="logistics_bronze",
    comment="Streaming logistics telemetry via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true"
    }
)
def logistics_bronze():
    """
    Ingest raw logistics telemetry events using Auto Loader.
    Tracks shipment events from creation through delivery.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", TELEMETRY_CHECKPOINT)
        .option("header", "true")
        .schema(LOGISTICS_SCHEMA)
        .load(TELEMETRY_PATH)
    )
