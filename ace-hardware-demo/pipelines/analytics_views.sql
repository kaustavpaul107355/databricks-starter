-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analytics Layer: Consolidated Views for Unity Catalog Metrics
-- MAGIC 
-- MAGIC Two essential views built on gold tables:
-- MAGIC 1. **logistics_fact** - Unified fact table for detailed analysis
-- MAGIC 2. **supply_chain_kpi** - Executive KPI summary

-- COMMAND ----------

-- DBTITLE 1,Logistics Performance Fact Table
CREATE OR REFRESH LIVE TABLE logistics_fact
COMMENT "Unified logistics fact table with all dimensions and metrics for UC metrics and BI"
AS
SELECT 
  -- Event identifiers
  lg.event_id,
  lg.shipment_id,
  lg.event_ts AS delivery_timestamp,
  lg.ingest_date,
  
  -- Store dimension
  lg.store_id,
  lg.store_name,
  lg.store_city,
  lg.store_state,
  lg.region_id,
  lg.store_weekly_revenue,
  lg.store_latitude,
  lg.store_longitude,
  
  -- Vendor dimension  
  lg.vendor_id,
  lg.vendor_name,
  lg.vendor_type,
  lg.vendor_risk_tier,
  lg.vendor_on_time_pct,
  
  -- Carrier dimension
  lg.carrier,
  
  -- Shipment details
  lg.origin_city,
  lg.origin_state,
  lg.planned_arrival_ts,
  lg.shipment_total_value,
  
  -- Event metrics
  lg.delay_minutes,
  lg.delay_reason,
  lg.shipment_status,
  lg.event_type,
  lg.temperature_celsius,
  
  -- Store aggregates (from gold)
  sm.total_deliveries AS store_total_deliveries,
  sm.avg_delay_minutes AS store_avg_delay,
  sm.delayed_shipments AS store_delayed_shipments,
  ROUND((sm.delayed_shipments / sm.total_deliveries * 100), 2) AS store_delay_rate_pct,
  
  -- Vendor aggregates (from gold)
  vp.total_deliveries AS vendor_total_deliveries,
  vp.delayed_deliveries AS vendor_delayed_deliveries,
  ROUND((vp.delayed_deliveries / vp.total_deliveries * 100), 2) AS vendor_delay_rate_pct,
  
  -- Carrier aggregates (from gold)
  cp.total_deliveries AS carrier_total_deliveries,
  cp.avg_delay_minutes AS carrier_avg_delay,
  ROUND((cp.delayed_deliveries / cp.total_deliveries * 100), 2) AS carrier_delay_rate_pct,
  
  -- Derived flags for filtering
  CASE WHEN lg.delay_minutes > 0 THEN 1 ELSE 0 END AS is_delayed,
  CASE WHEN lg.delay_minutes > 60 THEN 1 ELSE 0 END AS is_severely_delayed,
  CASE WHEN lg.vendor_type = 'ACE' THEN 1 ELSE 0 END AS is_ace_vendor,
  CASE WHEN lg.temperature_celsius IS NOT NULL THEN 1 ELSE 0 END AS is_temp_monitored,
  
  -- Risk scoring
  CASE 
    WHEN sm.avg_delay_minutes > 100 THEN 'HIGH'
    WHEN sm.avg_delay_minutes > 50 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS store_risk_tier,
  
  ROUND(sm.store_weekly_revenue * (sm.delayed_shipments / sm.total_deliveries), 2) AS revenue_at_risk,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS fact_refresh_ts
  
FROM LIVE.logistics_silver lg

-- Join store metrics
LEFT JOIN LIVE.store_delay_metrics sm
  ON lg.store_id = sm.store_id
  
-- Join vendor metrics  
LEFT JOIN LIVE.vendor_performance vp
  ON lg.vendor_id = vp.vendor_id
  AND lg.region_id = vp.region_id
  
-- Join carrier metrics
LEFT JOIN LIVE.carrier_performance cp
  ON lg.carrier = cp.carrier
  
WHERE lg.event_type = 'DELIVERED';

-- COMMAND ----------

-- DBTITLE 1,Supply Chain KPI Summary
CREATE OR REFRESH LIVE TABLE supply_chain_kpi
COMMENT "Executive KPI rollup by region, vendor type, and carrier for dashboards"
AS
SELECT
  region_id,
  vendor_type,
  carrier,
  
  -- Volume metrics
  COUNT(*) AS total_deliveries,
  SUM(is_delayed) AS delayed_count,
  SUM(is_severely_delayed) AS severely_delayed_count,
  
  -- Performance metrics
  ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
  ROUND((SUM(is_delayed) / COUNT(*) * 100), 2) AS delay_rate_pct,
  ROUND((SUM(is_severely_delayed) / COUNT(*) * 100), 2) AS severe_delay_rate_pct,
  ROUND(((COUNT(*) - SUM(is_delayed)) / COUNT(*) * 100), 2) AS on_time_rate_pct,
  
  -- Value metrics  
  ROUND(SUM(shipment_total_value), 2) AS total_value_delivered,
  ROUND(AVG(shipment_total_value), 2) AS avg_shipment_value,
  ROUND(SUM(revenue_at_risk), 2) AS total_revenue_at_risk,
  
  -- Quality metrics
  ROUND(AVG(temperature_celsius), 2) AS avg_temperature,
  SUM(is_temp_monitored) AS temp_monitored_count,
  
  -- Vendor breakdown
  SUM(is_ace_vendor) AS ace_vendor_shipments,
  COUNT(*) - SUM(is_ace_vendor) AS non_ace_vendor_shipments,
  
  -- Risk distribution
  SUM(CASE WHEN store_risk_tier = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_stores_count,
  SUM(CASE WHEN store_risk_tier = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_stores_count,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS kpi_refresh_ts
  
FROM LIVE.logistics_fact

GROUP BY region_id, vendor_type, carrier
ORDER BY delay_rate_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Usage Examples
-- MAGIC 
-- MAGIC ### Unity Catalog Metrics
-- MAGIC 
-- MAGIC **On-Time Delivery Rate**:
-- MAGIC ```sql
-- MAGIC SELECT region_id, on_time_rate_pct 
-- MAGIC FROM kaustavpaul_demo.ace_demo.supply_chain_kpi
-- MAGIC ```
-- MAGIC 
-- MAGIC **Revenue at Risk by Region**:
-- MAGIC ```sql
-- MAGIC SELECT region_id, SUM(revenue_at_risk) 
-- MAGIC FROM kaustavpaul_demo.ace_demo.logistics_fact
-- MAGIC GROUP BY region_id
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Quick Analysis Queries
-- MAGIC 
-- MAGIC **High-Risk Stores**:
-- MAGIC ```sql
-- MAGIC SELECT store_name, store_city, store_risk_tier, revenue_at_risk
-- MAGIC FROM kaustavpaul_demo.ace_demo.logistics_fact
-- MAGIC WHERE store_risk_tier = 'HIGH'
-- MAGIC GROUP BY store_name, store_city, store_risk_tier, revenue_at_risk
-- MAGIC ORDER BY revenue_at_risk DESC
-- MAGIC ```
-- MAGIC 
-- MAGIC **ACE vs NON-ACE Performance**:
-- MAGIC ```sql
-- MAGIC SELECT 
-- MAGIC   vendor_type,
-- MAGIC   AVG(on_time_rate_pct) as avg_on_time_rate,
-- MAGIC   AVG(delay_rate_pct) as avg_delay_rate,
-- MAGIC   SUM(total_value_delivered) as total_value
-- MAGIC FROM kaustavpaul_demo.ace_demo.supply_chain_kpi
-- MAGIC GROUP BY vendor_type
-- MAGIC ```
