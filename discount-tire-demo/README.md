# Discount Tire Executive Brief Demo

![Discount Tire](./DT_logo.svg)

An end-to-end AI-powered executive dashboard demo built on the Databricks Data Intelligence Platform. Features natural language querying via Genie, real-time analytics, interactive visualizations, and geospatial insights—all powered by Unity Catalog data.

## 🎯 Overview

This demo showcases how Databricks can power modern executive dashboards for retail businesses. It combines:

- **Databricks Genie**: Natural language to SQL for intuitive data access
- **Unity Catalog**: Centralized data governance and lineage
- **SQL Warehouses**: Fast, scalable query execution
- **Databricks Apps**: Secure, authenticated web applications
- **Delta Lake**: Reliable, performant data storage

The demo is tailored for **Discount Tire**, America's largest independent tire retailer, demonstrating how AI can transform executive decision-making.

## 📁 Repository Structure

```
discount-tire-demo/
├── data/                    # Synthetic CSV data (git-ignored)
│   ├── customers.csv
│   ├── products.csv
│   ├── sales.csv
│   ├── inventory.csv
│   ├── services.csv
│   ├── stores.csv
│   ├── promotions.csv
│   ├── appointments.csv
│   ├── surveys.csv
│   ├── feedback_topics.csv
│   ├── inventory_movements.csv
│   └── store_kpis.csv
├── generate_mock_data.py    # Data generator (deterministic, seeded)
├── notebooks/               # Databricks notebooks for setup
│   └── discount_tire_demo.py
├── ui/                      # Databricks App (React + Python backend)
│   ├── backend/
│   │   └── server.py        # HTTP server with Genie + SQL endpoints
│   ├── src/
│   │   └── app/
│   │       └── components/  # React components
│   ├── dist/                # Build output (git-ignored)
│   ├── app.yaml             # Local config (git-ignored)
│   ├── app_git.yaml         # Git-safe config template
│   └── README.md            # Comprehensive app documentation
├── ui_deploy/               # Deployment artifacts (git-ignored)
├── DT_logo.svg              # Discount Tire logo
├── DBX_logo.svg             # Databricks logo
└── README.md                # This file
```

## 🚀 Quick Start

### 1. Generate Mock Data

Create synthetic datasets (customers, sales, inventory, etc.):

```bash
python generate_mock_data.py
```

**Output**: 12 CSV files with ~6,500 total rows of realistic retail data spanning 2025.

### 2. Upload Data to Databricks

```bash
# Create directory
databricks fs mkdirs dbfs:/FileStore/discount-tire-demo

# Upload all CSVs
for file in data/*.csv; do
  databricks fs cp "$file" "dbfs:/FileStore/discount-tire-demo/$(basename $file)" --overwrite
done
```

### 3. Run Setup Notebook

1. Open `notebooks/discount_tire_demo.py` in Databricks
2. Attach to a cluster
3. Run all cells to:
   - Create Unity Catalog schema (`kaustavpaul_demo.dtc_demo`)
   - Ingest CSVs into Delta tables
   - Create enriched views (`vw_sales_enriched`, `vw_revenue_growth`)

### 4. Deploy the App

See [`ui/README.md`](./ui/README.md) for detailed instructions:

```bash
cd ui
npm install
npm run build
databricks workspace import-dir . /Workspace/Users/<your-email>/discount-tire-demo/ui --overwrite
databricks apps deploy dtc-exec-view-app --mode SNAPSHOT --source-code-path /Workspace/Users/<your-email>/discount-tire-demo/ui
```

## 📊 Data Overview

### Datasets

| Dataset | Rows | Description |
|---------|------|-------------|
| `customers.csv` | ~350 | Customer profiles with satisfaction scores (3.2-5.0) |
| `products.csv` | 12 | Tire models and service offerings |
| `sales.csv` | ~1,000 | Sales transactions across 2025 |
| `inventory.csv` | ~250 | Product stock levels across 25 stores |
| `services.csv` | ~500 | Service appointments and revenue |
| `stores.csv` | 25 | Store locations (realistic city/region data) |
| `promotions.csv` | 5 | Marketing campaigns |
| `appointments.csv` | ~800 | Customer appointment bookings |
| `surveys.csv` | ~1,200 | Customer satisfaction surveys |
| `feedback_topics.csv` | ~900 | Categorized customer feedback |
| `inventory_movements.csv` | ~1,800 | Stock transfers and adjustments |
| `store_kpis.csv` | 300 | Monthly KPIs for all stores |

### Unity Catalog Schema

**Catalog**: `kaustavpaul_demo`  
**Schema**: `dtc_demo`

**Key Views**:
- `vw_sales_enriched`: Joined sales data with product, store, and customer details
- `vw_revenue_growth`: Month-over-month revenue growth calculations

## 🎨 UI Features

### 1. Executive Summary Tab
- **AI Chat Interface**: Natural language queries via Genie
- **Voice Input**: Speech-to-text with enhanced TTS readout
- **Suggested Questions**: Quick-access query templates
- **KPI Metrics**: Revenue, growth, units, satisfaction, inventory risk
- **Charts**: Revenue trends, top products, inventory health, satisfaction

### 2. Revenue Analytics Tab
- Revenue performance metrics
- Month-over-month growth trends
- Category breakdown (tires vs. services)
- Current month contextualization

### 3. Operations Tab
- Inventory health metrics
- Store performance comparison
- Operational efficiency indicators

### 4. Customer Insights Tab
- Satisfaction score trends
- Customer feedback analysis
- Regional satisfaction breakdown

### 5. Store Map Tab
- Interactive map with 25 store locations (Leaflet + OpenStreetMap)
- Store-level revenue and units sold
- High-level statistics tiles
- Geospatial performance visualization

## 🏗️ Technical Architecture

### Frontend
- **React 18** + TypeScript + Vite
- **Tailwind CSS** with glassmorphism design
- **Recharts** for data visualization
- **Leaflet** for interactive maps
- **Web Speech API** for voice input/output

### Backend
- **Python HTTP Server** (ThreadingHTTPServer)
- **Databricks SDK** for SQL Warehouse access
- **In-memory caching** with TTL (3-layer strategy)
- **Rate limiting** for Genie API (semaphore-based)
- **Error handling** with generic client messages

### Data
- **Unity Catalog** for governance
- **Delta Lake** for reliable storage
- **SQL Warehouse** for fast queries
- **Genie** for natural language access

## 🔐 Security & Governance

- **Authentication**: Databricks App authentication with user context
- **Authorization**: PAT tokens for Genie and SQL Warehouse access
- **TLS**: Verified HTTPS connections (opt-in insecure mode for testing)
- **Error Leakage**: Generic error messages to clients, detailed server logs
- **Secret Management**: Environment variables, git-ignored config files

## 📈 Metrics & KPIs

The dashboard tracks key metrics including:

- **Revenue**: Total sales and month-over-month growth
- **Units Sold**: Tire and service volume
- **Customer Satisfaction**: Average scores (1-5 scale)
- **Inventory Risk**: Low-stock product count
- **Store Performance**: Revenue and efficiency by location
- **Category Mix**: Tire sales vs. service revenue

## 🎤 AI Features

### Databricks Genie Integration
- Natural language to SQL translation
- Context-aware query understanding
- Formatted responses with tables
- Suggested follow-up questions

### Voice Capabilities
- **Input**: Browser-based speech recognition
- **Output**: Text-to-speech with natural cadence
- **UX**: Animated waveform, progress indicators, enhanced phrasing

## 🧪 Testing

### Backend Tests
```bash
cd ui
pytest backend/tests/test_genie_parsing.py
```

### Manual Testing
1. Test all dashboard tabs load correctly
2. Submit Genie queries via chat and voice
3. Verify map shows all 25 stores
4. Check user authentication tooltip
5. Validate tab transitions

## 📦 Dependencies

### Python (Backend)
- `databricks-sql-connector` - SQL Warehouse access
- No additional packages required (stdlib only)

### Node.js (Frontend)
- React, React-DOM, React-Router
- Recharts, Leaflet, React-Leaflet
- Tailwind CSS, Lucide Icons
- TypeScript, Vite

See [`ui/package.json`](./ui/package.json) for full list.

## 🚨 Troubleshooting

### Data Issues
- **Zero revenue**: Check date filters match data range (2025)
- **Missing stores**: Verify all 25 stores in `stores.csv`
- **Blank charts**: Confirm SQL Warehouse is running

### App Issues
- **Authentication errors**: Ensure Databricks App deployment (not local)
- **Genie failures**: Verify space ID and token permissions
- **Map not loading**: Check OpenStreetMap accessibility

### Performance
- **Slow queries**: Increase cache TTL values
- **Rate limits**: Reduce concurrent Genie requests
- **Memory usage**: Monitor cache size in production

## 📝 Configuration Files

### `ui/app.yaml` (git-ignored)
Local and workspace configuration with real credentials.

### `ui/app_git.yaml` (tracked)
Template with placeholders (`REPLACE_ME`) for GitHub.

**Never commit `app.yaml` to git!**

## 🎯 Use Cases

This demo showcases:

1. **Executive Decision-Making**: Real-time KPIs and trends
2. **Natural Language Analytics**: AI-powered data exploration
3. **Operational Insights**: Store and inventory performance
4. **Customer Understanding**: Satisfaction trends and feedback
5. **Geospatial Analysis**: Location-based performance metrics

## 🔄 Data Updates

To refresh data:

1. Regenerate CSVs: `python generate_mock_data.py`
2. Upload to DBFS (see Quick Start)
3. Re-run notebook ingestion cells
4. Dashboard auto-updates on next cache expiry

## 📚 Additional Resources

- [Databricks Genie Documentation](https://docs.databricks.com/genie/)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [SQL Warehouses Documentation](https://docs.databricks.com/sql/)

## 🤝 Contributing

This is a demo project. For production use, consider:

- Implementing role-based access control (RBAC)
- Adding comprehensive error boundaries
- Setting up CI/CD pipelines
- Implementing monitoring and observability
- Adding E2E tests (Playwright, Cypress)
- Optimizing cache strategies for scale
- Adding data refresh scheduling

## 📞 Support

For questions or issues, contact the Databricks Field Engineering team.

---

**Built on Databricks Data Intelligence Platform** | **Powered by Databricks Genie**
