
# AI-Powered Executive Business Brief (Databricks App)

This app is a Databricks App that provides an executive brief dashboard for Discount Tire:
KPIs, operational metrics, revenue analytics, customer insights, a map view, and an AI
chat interface powered by Genie. Dashboard sections are served from direct SQL queries
against Unity Catalog data, with caching to keep performance consistent.

## Architecture

- **Frontend**: React + Vite + Tailwind + Recharts + Leaflet
- **Backend**: FastAPI-style HTTP server (`backend/server.py`)
  - `/api/genie/query`: Natural language queries via Genie
  - `/api/dashboard/*`: Live dashboard endpoints using SQL Warehouse
- **Data**: Unity Catalog tables and views (`kaustavpaul_demo.dtc_demo`)
  - `vw_sales_enriched`, `vw_revenue_growth`, plus base tables (e.g., `inventory`, `stores`)

## Run Locally

Install dependencies:

```bash
npm i
```

Run the dev server:

```bash
npm run dev
```

## Databricks App Configuration

The app is deployed using `ui/app.yaml` (local/workspace config, ignored by git).
For GitHub, use `ui/app_git.yaml` (sanitized placeholders).

- `DATABRICKS_HOST` (e.g., `e2-demo-field-eng.cloud.databricks.com`)
- `DATABRICKS_SQL_HTTP_PATH` (SQL Warehouse HTTP path)
- `DATABRICKS_TOKEN_FOR_SQL` (PAT for SQL warehouse access)
- `GENIE_SPACE_ID`
- `DATABRICKS_TOKEN_FOR_GENIE`

## Dashboard Endpoints

All dashboard endpoints use direct SQL and cache payloads in memory:

- `GET /api/dashboard/kpis`
- `GET /api/dashboard/charts`
- `GET /api/dashboard/revenue`
- `GET /api/dashboard/operations`
- `GET /api/dashboard/customers`
- `GET /api/dashboard/map`

Caching is controlled by:

- `SQL_CACHE_TTL_SECONDS` (SQL result cache, default 300s)
- `DASHBOARD_CACHE_TTL_SECONDS` (payload cache, default 120s)

## Genie Endpoint

`POST /api/genie/query` accepts:

```json
{ "question": "What was revenue growth last quarter?" }
```

Response includes:

```json
{ "summary": "...", "table": { "columns": [], "rows": [] } }
```

## Map View

The map tab uses Leaflet + OpenStreetMap tiles. Store coordinates are derived from state
centroids (via SQL) unless you add real `latitude` and `longitude` to the `stores` table.

## Data Refresh

Dashboard data is queried directly from Unity Catalog tables/views. If you regenerate
CSV files locally, update the corresponding tables in Databricks to refresh the app.

## Testing

Backend Genie parsing tests live in:

- `ui/backend/tests/test_genie_parsing.py`

Run with:

```bash
pytest ui/backend/tests/test_genie_parsing.py
```
  