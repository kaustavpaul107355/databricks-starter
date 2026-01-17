# Unity Catalog Metrics Setup Guide

## Analytics Layer

Two consolidated SQL views built on gold tables:

### 1. `logistics_fact`
**Unified fact table** combining all dimensions and metrics for detailed analysis.

**Key Features**:
- All dimensions: stores, vendors, carriers, shipments
- Individual delivery metrics: delay, value, temperature
- Aggregated context: store/vendor/carrier totals from gold tables
- Risk scoring: store risk tiers, revenue at risk
- Filter flags: is_delayed, is_ace_vendor, is_temp_monitored

**Use For**: Detailed drill-down, cohort analysis, ML features, UC metric definitions

### 2. `supply_chain_kpi`
**Executive KPI rollup** by region, vendor type, and carrier.

**Key Metrics**:
- Performance: on_time_rate_pct, delay_rate_pct, avg_delay_minutes
- Volume: total_deliveries, delayed_count, severely_delayed_count
- Value: total_value_delivered, total_revenue_at_risk
- Quality: temp_monitored_count, avg_temperature

**Use For**: Executive dashboards, trend monitoring, comparative analytics

## Creating Unity Catalog Metrics

### Step 1: Add SQL Views to DLT Pipeline

In your DLT pipeline configuration, add:
```
/Workspace/Users/kaustav.paul@databricks.com/ace-demo/pipelines/analytics_views.sql
```

### Step 2: Define Metrics in Unity Catalog

Navigate to: **Catalog** → **kaustavpaul_demo** → **ace_demo** → Select table → **Create Metric**

#### Metric 1: On-Time Delivery Rate

```sql
-- Table: supply_chain_kpi
Name: on_time_delivery_rate
Expression: AVG(on_time_rate_pct)
Dimensions: region_id, vendor_type, carrier
```

#### Metric 2: Average Delay Minutes

```sql
-- Table: supply_chain_kpi
Name: avg_delay_minutes
Expression: AVG(avg_delay_minutes)
Dimensions: region_id, carrier
```

#### Metric 3: Revenue at Risk

```sql
-- Table: logistics_fact
Name: revenue_at_risk
Expression: SUM(revenue_at_risk)
Dimensions: region_id, store_risk_tier
Filter: store_risk_tier IN ('HIGH', 'MEDIUM')
```

#### Metric 4: Severe Delay Rate

```sql
-- Table: supply_chain_kpi
Name: severe_delay_rate
Expression: AVG(severe_delay_rate_pct)
Dimensions: region_id, carrier
```

## Sample Analysis Queries

### High-Risk Stores

```sql
SELECT 
  store_name,
  store_city,
  store_state,
  store_risk_tier,
  ROUND(revenue_at_risk, 2) as revenue_at_risk,
  ROUND(store_delay_rate_pct, 2) as delay_rate
FROM kaustavpaul_demo.ace_demo.logistics_fact
WHERE store_risk_tier = 'HIGH'
GROUP BY store_name, store_city, store_state, store_risk_tier, revenue_at_risk, store_delay_rate_pct
ORDER BY revenue_at_risk DESC
LIMIT 10;
```

### Regional Performance Comparison

```sql
SELECT 
  region_id,
  ROUND(AVG(on_time_rate_pct), 2) as avg_on_time_rate,
  ROUND(AVG(delay_rate_pct), 2) as avg_delay_rate,
  SUM(total_deliveries) as total_volume,
  ROUND(SUM(total_value_delivered), 2) as total_value
FROM kaustavpaul_demo.ace_demo.supply_chain_kpi
GROUP BY region_id
ORDER BY avg_on_time_rate DESC;
```

### ACE vs NON-ACE Vendor Performance

```sql
SELECT 
  vendor_type,
  COUNT(DISTINCT carrier) as carriers_used,
  ROUND(AVG(on_time_rate_pct), 2) as avg_on_time_rate,
  ROUND(AVG(delay_rate_pct), 2) as avg_delay_rate,
  ROUND(SUM(total_value_delivered), 2) as total_business_value,
  ROUND(SUM(total_revenue_at_risk), 2) as total_revenue_at_risk
FROM kaustavpaul_demo.ace_demo.supply_chain_kpi
GROUP BY vendor_type
ORDER BY avg_on_time_rate DESC;
```

### Carrier Benchmarking

```sql
SELECT 
  carrier,
  SUM(total_deliveries) as shipments,
  ROUND(AVG(on_time_rate_pct), 2) as on_time_rate,
  ROUND(AVG(avg_delay_minutes), 2) as avg_delay,
  ROUND(AVG(severe_delay_rate_pct), 2) as severe_delay_rate,
  ROUND(SUM(total_value_delivered), 2) as total_value
FROM kaustavpaul_demo.ace_demo.supply_chain_kpi
GROUP BY carrier
ORDER BY on_time_rate DESC;
```

### Temperature-Sensitive Shipment Analysis

```sql
SELECT 
  region_id,
  carrier,
  SUM(is_temp_monitored) as temp_monitored_shipments,
  ROUND(AVG(CASE WHEN is_temp_monitored = 1 THEN temperature_celsius END), 2) as avg_temp,
  ROUND(AVG(CASE WHEN is_temp_monitored = 1 THEN delay_minutes END), 2) as avg_delay
FROM kaustavpaul_demo.ace_demo.logistics_fact
WHERE is_temp_monitored = 1
GROUP BY region_id, carrier
ORDER BY avg_delay DESC;
```

## Dashboard Examples

### Executive Dashboard (supply_chain_kpi)

**KPI Cards**:
- On-Time Rate: `AVG(on_time_rate_pct)`
- Total Deliveries: `SUM(total_deliveries)`
- Revenue at Risk: `SUM(total_revenue_at_risk)`

**Charts**:
- Bar Chart: On-time rate by region
- Line Chart: Delay rate trend over time
- Pie Chart: ACE vs NON-ACE delivery volume

### Store Risk Dashboard (logistics_fact)

**Map Visualization**:
- Location: `store_latitude`, `store_longitude`
- Color: `store_risk_tier`
- Size: `revenue_at_risk`

**Tables**:
- Top 20 high-risk stores
- Store performance by region

### Vendor Scorecard (supply_chain_kpi)

**Table**:
- Vendor type, region, on-time rate, delay rate, total value
- Filters: vendor_type, region_id

**Scatter Plot**:
- X-axis: `on_time_rate_pct`
- Y-axis: `total_value_delivered`
- Color: `vendor_type`

## Best Practices

1. **Use SQL views for metrics**: Easier to understand than Python for business users
2. **Keep views simple**: 2 well-designed views better than 10 specialized ones
3. **Pre-calculate rates**: Don't make users compute percentages
4. **Add risk scoring**: Combine multiple metrics into actionable tiers
5. **Include metadata**: Refresh timestamps for data freshness tracking
6. **Document inline**: SQL comments explain business logic
7. **Test with queries**: Validate metric calculations before production

## Demo Narrative

**For ACE Hardware Stakeholders:**

> "We've consolidated your gold metrics into 2 analytics views:
> 
> 1. **Logistics Fact** - Your detailed data for deep analysis. Every delivery with full context about the store, vendor, carrier, and risk level. Use this for root cause analysis.
> 
> 2. **Supply Chain KPI** - Your executive summary. Pre-calculated on-time rates, delay metrics, and revenue at risk rolled up by region and vendor type. Perfect for leadership dashboards.
> 
> These views power Unity Catalog metrics, so your entire organization uses the same definitions for 'on-time rate' or 'revenue at risk' - no more spreadsheet disagreements."

---

**Tech Stack**: Delta Live Tables | SQL | Unity Catalog | Delta Lake
