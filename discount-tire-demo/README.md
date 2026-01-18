# Discount Tire Executive Brief Demo

This folder contains mock data and a Databricks notebook outline for an AI-powered executive brief demo tailored to Discount Tire's retail and service business.

## What is included

- `data/` synthetic CSVs for customers, products, sales, inventory, services,
  stores, promotions, appointments, surveys, feedback topics, inventory
  movements, and store KPIs
- `generate_mock_data.py` data generator (deterministic with a fixed seed)
- `notebooks/discount_tire_demo.py` Databricks notebook outline
- `ui/` Databricks App (React + backend server) for the executive brief dashboard

## Executive Brief App

The app lives in `ui/` and includes:

- React UI + charts + map view
- Backend server for Genie and SQL Warehouse endpoints
- Build output in `ui/dist` (ignored by git)

Configuration files:

- `ui/app.yaml` local/workspace configuration (ignored by git)
- `ui/app_git.yaml` safe, sanitized version for GitHub

See `ui/README.md` for architecture, endpoints, and deployment notes.

## Generate the mock data

From the repo root:

```bash
python discount-tire-demo/generate_mock_data.py
```

This produces:

- `customers.csv` (~350 rows, satisfaction score 3.2-5.0)
- `products.csv` (12 rows)
- `sales.csv` (~1000 rows across 2025, includes `promotion_id`)
- `inventory.csv` (~250 rows across 25 stores)
- `services.csv` (~500 rows across 2025)
- `stores.csv` (25 rows, store metadata)
- `promotions.csv` (5 rows)
- `appointments.csv` (~800 rows across 2025)
- `surveys.csv` (~1200 rows across 2025)
- `feedback_topics.csv` (~900 rows across 2025)
- `inventory_movements.csv` (~1800 rows across 2025)
- `store_kpis.csv` (25 stores x 12 months)

## Load to Databricks (CLI + Notebook)

Example CLI uploads:

```bash
databricks fs mkdirs dbfs:/FileStore/discount-tire-demo
databricks fs cp discount-tire-demo/data/customers.csv dbfs:/FileStore/discount-tire-demo/customers.csv
databricks fs cp discount-tire-demo/data/products.csv dbfs:/FileStore/discount-tire-demo/products.csv
databricks fs cp discount-tire-demo/data/sales.csv dbfs:/FileStore/discount-tire-demo/sales.csv
databricks fs cp discount-tire-demo/data/inventory.csv dbfs:/FileStore/discount-tire-demo/inventory.csv
databricks fs cp discount-tire-demo/data/services.csv dbfs:/FileStore/discount-tire-demo/services.csv
databricks fs cp discount-tire-demo/data/stores.csv dbfs:/FileStore/discount-tire-demo/stores.csv
databricks fs cp discount-tire-demo/data/promotions.csv dbfs:/FileStore/discount-tire-demo/promotions.csv
databricks fs cp discount-tire-demo/data/appointments.csv dbfs:/FileStore/discount-tire-demo/appointments.csv
databricks fs cp discount-tire-demo/data/surveys.csv dbfs:/FileStore/discount-tire-demo/surveys.csv
databricks fs cp discount-tire-demo/data/feedback_topics.csv dbfs:/FileStore/discount-tire-demo/feedback_topics.csv
databricks fs cp discount-tire-demo/data/inventory_movements.csv dbfs:/FileStore/discount-tire-demo/inventory_movements.csv
databricks fs cp discount-tire-demo/data/store_kpis.csv dbfs:/FileStore/discount-tire-demo/store_kpis.csv
```

Then open `notebooks/discount_tire_demo.py` in Databricks and run the steps to create the catalog, ingest the CSVs, and build the dashboard queries.

## Metric and dashboard ideas

- Total revenue
- Revenue growth (period over period)
- Tire unit sales
- Service attach rate
- Inventory health index
- Satisfaction by region
