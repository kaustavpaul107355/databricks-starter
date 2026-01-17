# ACE Hardware Logistics DLT Pipeline

Production-ready Delta Live Tables pipeline for real-time supply chain analytics.

## Overview

This pipeline demonstrates:
- **Streaming ingestion** with Auto Loader (Zero Bus pattern)
- **Medallion architecture** (Bronze → Silver → Gold)
- **Data quality enforcement** via DLT expectations
- **Real US geography** with GPS-tracked routes
- **Rich analytics** for Fulfillment & Logistics Optimization (FLO)

## Quick Start

### 1. Generate Mock Data

```bash
python scripts/generate_data.py \
  --num-shipments 1200 \
  --num-events 1500 \
  --num-stores 250 \
  --num-vendors 40 \
  --num-products 500 \
  --seed 42 \
  --output-dir data
```

**Generates:**
- 6,480 tracking events across shipment lifecycle
- 250 stores with real US cities and GPS coordinates
- 40 vendors with performance metrics
- 500 products across 9 categories
- 10,767 shipment line items
- 1,200 shipments with origin/destination details

### 2. Upload to Databricks Volumes

Upload generated files to:
```
/Volumes/kaustavpaul_demo/ace_demo/ace_files/data/
  ├── telemetry/
  │   └── logistics_telemetry.csv
  └── dimensions/
      ├── stores.csv
      ├── vendors.csv
      ├── shipments.csv
      ├── products.csv
      └── shipment_line_items.csv
```

### 3. Create DLT Pipeline

**In Databricks UI:**
1. Navigate to **Workflows** → **Delta Live Tables** → **Create Pipeline**
2. Configure:
   - **Catalog**: `kaustavpaul_demo`
   - **Target Schema**: `ace_demo`
   - **Source Code**: Add notebook paths:
     ```
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/config.py
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/bronze_logistics.py
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/bronze_dimensions.py
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/silver_logistics.py
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/gold_flo_metrics.py
     /Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/analytics_views.sql
     ```

## Architecture

### Bronze Layer (Raw Ingestion)
- **`logistics_bronze`**: Streaming telemetry via Auto Loader
- **`stores_bronze`**: Store master data
- **`vendors_bronze`**: Vendor master data
- **`shipments_bronze`**: Shipment details
- **`products_bronze`**: Product catalog
- **`shipment_line_items_bronze`**: Shipment line items

### Silver Layer (Enriched & Quality-Checked)
- **`logistics_silver`**: Telemetry enriched with stores, vendors, shipments
  - Data quality: Drops records with missing store_id, vendor_id, invalid statuses

### Gold Layer (Business Metrics)
- **`store_delay_metrics`**: Store-level delays for stockout risk
- **`vendor_performance`**: Vendor scorecarding by region
- **`carrier_performance`**: Carrier benchmarking
- **`product_category_metrics`**: Category-level delivery analysis

### Analytics Layer (SQL Views for UC Metrics)
- **`logistics_fact`**: Unified fact table with all dimensions, metrics, and risk scoring
- **`supply_chain_kpi`**: Executive KPI rollup by region/vendor/carrier

## Data Model

### Logistics Telemetry Events
```
SHIPMENT_CREATED → DEPARTED_WAREHOUSE → IN_TRANSIT → 
ARRIVED_DC → OUT_FOR_DELIVERY → DELIVERED
```

**Key Fields:**
- GPS coordinates (interpolated along route)
- Event types and timestamps
- Delay reasons (WEATHER, TRAFFIC, MECHANICAL_FAILURE, etc.)
- Carrier assignments
- Temperature monitoring for sensitive goods
- Shipment values

### Real Geography
- 250 stores across 50 US states with actual city GPS coordinates
- Region mapping follows US Census Bureau definitions:
  - **MIDWEST**: IL, IN, MI, OH, WI, MN, IA, MO, ND, SD, NE, KS
  - **SOUTH**: TX, FL, GA, NC, VA, TN, LA, KY, SC, AL, MS, AR, OK
  - **NORTHEAST**: NY, PA, NJ, MA, CT, RI, NH, VT, ME
  - **WEST**: CA, WA, OR, AZ, CO, NV, UT, ID, MT, WY, NM, HI, AK

## Use Cases (ACE Hardware FLO)

1. **Stockout Risk Prediction**
   - Store-level delay aggregations
   - Correlate delays with store revenue and location

2. **Vendor Performance Management**
   - ACE vs NON-ACE vendor comparison
   - Regional performance variations
   - Risk tier analysis

3. **Estimated Arrival Date (EAD) Model**
   - Historical delay patterns by carrier, route, weather
   - Temperature spike correlation with delays

4. **Zero Bus Ingest Pattern**
   - Auto Loader streaming from Volumes (no Kafka/message queue)
   - Simplified architecture for ASN telemetry

## File Structure

```
ace-hardware-demo/
├── pipelines/
│   ├── config.py              # Centralized configuration
│   ├── bronze_logistics.py    # Streaming telemetry ingestion
│   ├── bronze_dimensions.py   # Batch dimension tables
│   ├── silver_logistics.py    # Enriched telemetry
│   ├── gold_flo_metrics.py    # Business aggregations
│   └── analytics_views.sql    # Analytics layer (SQL views)
├── scripts/
│   ├── generate_data.py       # Data generator
│   └── sync_with_curl.sh      # Workspace sync utility
├── data/                      # Generated datasets
│   ├── telemetry/
│   └── dimensions/
└── README.md
```

## Workspace Location

Code is deployed to:
```
/Workspace/Users/kaustav.paul@databricks.com/ace-demo/
```

## Sync Code to Workspace

```bash
DATABRICKS_TOKEN=<your-token> bash scripts/sync_with_curl.sh
```

## Demo Talking Points

**For ACE Hardware Stakeholders:**

1. **"Zero Bus Ingest"**: Lightweight streaming from Volumes (no complex message queue)
2. **Real-time visibility**: Track shipments from warehouse to store
3. **Data quality built-in**: DLT expectations drop bad vendor/store data automatically
4. **Analytics-ready**: Gold tables power FLO dashboards and ML models
5. **Temperature compliance**: Monitor sensitive goods (Paint, Seasonal)
6. **Vendor accountability**: Compare ACE vs NON-ACE vendor performance
7. **Route optimization**: GPS tracking enables carrier performance analysis

---

**Tech Stack**: Databricks DLT | Auto Loader | Delta Lake | Unity Catalog
