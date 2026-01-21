# AI-Powered Executive Business Brief (Databricks App)

A modern, AI-powered executive dashboard for Discount Tire built on Databricks. Features natural language querying via Genie, real-time analytics, interactive visualizations, and a geospatial store map—all powered by Unity Catalog data.

## 🎯 Features

- **AI Chat Interface**: Natural language queries powered by Databricks Genie
- **Voice Input**: Speech-to-text for hands-free queries with enhanced TTS output
- **Live Dashboards**: Real-time KPIs, revenue analytics, operations, and customer insights
- **Interactive Map**: Geospatial visualization of store locations and performance
- **Authenticated Access**: Pull real user information from Databricks App context
- **Responsive UI**: Modern glassmorphism design with smooth transitions

## 🏗️ Architecture

### Frontend
- **Framework**: React 18 + TypeScript + Vite
- **Styling**: Tailwind CSS with custom glassmorphism effects
- **Charts**: Recharts for data visualizations
- **Maps**: Leaflet + OpenStreetMap tiles
- **State Management**: React hooks + local state
- **Code Splitting**: Lazy loading for route-based chunks (Revenue, Operations, Customers, Map)
- **Performance**: Gzip compression, optimized bundle splitting

### Backend (`backend/server.py`)
- **Server**: Python HTTP server (ThreadingHTTPServer)
- **Endpoints**:
  - `/api/user` - Authenticated user information
  - `/api/genie/query` - Natural language queries via Genie
  - `/api/dashboard/*` - Live dashboard data endpoints
- **Data Access**: 
  - Direct SQL queries to Databricks SQL Warehouse with connection pooling
  - Fallback to Genie for ad-hoc queries
- **Caching**: Multi-layer in-memory caching with TTL
- **Concurrency**: Semaphore-based rate limiting for Genie API
- **Optimization**: Response compression (gzip), SQL connection pool

### Data Layer
- **Catalog**: `kaustavpaul_demo.dtc_demo`
- **Views**:
  - `vw_sales_enriched` - Enriched sales data with products, stores, customers
  - `vw_revenue_growth` - Month-over-month revenue growth metrics
- **Tables**: `customers`, `products`, `sales`, `inventory`, `stores`, `services`, etc.

## 📦 Project Structure

```
ui/
├── backend/
│   ├── server.py              # Main HTTP server with gzip & pooling
│   ├── db_pool.py             # SQL connection pool manager
│   ├── main.py                # Entry point
│   ├── validate_genie_outputs.py
│   └── tests/
│       └── test_genie_parsing.py
├── src/
│   ├── app/
│   │   ├── App.tsx            # Main app with lazy loading
│   │   └── components/
│   │       ├── Header.tsx     # App header with user auth
│   │       ├── TabNavigation.tsx  # Floating tab buttons
│   │       ├── ExecutiveSummary.tsx
│   │       ├── AIInteractionPanel.tsx  # Voice/text input
│   │       ├── KPIMetrics.tsx
│   │       ├── ChartSection.tsx
│   │       ├── RevenueAnalytics.tsx  # Lazy loaded
│   │       ├── Operations.tsx         # Lazy loaded
│   │       ├── CustomerInsights.tsx   # Lazy loaded
│   │       ├── MapView.tsx            # Lazy loaded + Leaflet
│   │       └── GovernanceFooter.tsx
│   ├── test/
│   │   ├── setup.ts           # Vitest setup
│   │   ├── Header.test.tsx
│   │   ├── TabNavigation.test.tsx
│   │   ├── GovernanceFooter.test.tsx
│   │   └── utils.test.ts
│   ├── assets/
│   │   ├── DT_logo.svg
│   │   └── DBX_logo.svg
│   ├── styles/
│   │   └── *.css
│   └── main.tsx
├── dist/                      # Build output (git-ignored)
├── app.yaml                   # Local/workspace config (git-ignored)
├── app_git.yaml               # Sanitized config for git
├── vitest.config.ts           # Test configuration
├── package.json
└── README.md
```

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ and npm
- Python 3.9+
- Access to a Databricks workspace
- Databricks SQL Warehouse
- Databricks Genie space

### Local Development

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Configure environment**:
   Copy `app_git.yaml` to `app.yaml` and fill in your credentials:
   ```yaml
   env:
     - name: DATABRICKS_HOST
       value: "your-workspace.cloud.databricks.com"
     - name: DATABRICKS_SQL_HTTP_PATH
       value: "/sql/1.0/warehouses/your-warehouse-id"
     - name: DATABRICKS_TOKEN_FOR_SQL
       value: "your-pat-token"
     - name: GENIE_SPACE_ID
       value: "your-genie-space-id"
     - name: DATABRICKS_TOKEN_FOR_GENIE
       value: "your-genie-pat-token"
   ```

3. **Run development server**:
   ```bash
   npm run dev
   ```

4. **Build for production**:
   ```bash
   npm run build
   ```

### Databricks App Deployment

1. **Build the UI**:
   ```bash
   npm run build
   ```

2. **Upload to workspace**:
   ```bash
   databricks workspace import-dir ui /Workspace/Users/<your-email>/discount-tire-demo/ui --overwrite
   ```

3. **Deploy the app**:
   ```bash
   databricks apps deploy dtc-exec-view-app \
     --mode SNAPSHOT \
     --source-code-path /Workspace/Users/<your-email>/discount-tire-demo/ui
   ```

## 🔌 API Endpoints

### User Authentication
**GET** `/api/user`

Returns authenticated user information from Databricks App context.

**Response:**
```json
{
  "name": "Kaustav Paul",
  "email": "kaustav.paul@databricks.com",
  "role": "Executive Viewer"
}
```

### Genie Query
**POST** `/api/genie/query`

Submit natural language queries to Databricks Genie.

**Request:**
```json
{
  "question": "What is the total revenue for the last quarter?"
}
```

**Response:**
```json
{
  "summary": "The total revenue for the last quarter was $76,685.00...",
  "table": {
    "columns": ["total_revenue"],
    "rows": [["76685.00"]]
  }
}
```

### Dashboard Endpoints

All dashboard endpoints use **GET** requests and return live data with caching:

- `/api/dashboard/kpis` - Key performance indicators
- `/api/dashboard/charts` - Chart data for executive summary
- `/api/dashboard/revenue` - Revenue analytics
- `/api/dashboard/operations` - Operational metrics
- `/api/dashboard/customers` - Customer insights
- `/api/dashboard/map` - Store locations and performance

**Example Response** (`/api/dashboard/kpis`):
```json
{
  "currentMonthLabel": "December 2025",
  "revenue": "$123,456",
  "growth": "+15.3%",
  "units": "1,234",
  "satisfaction": "4.2",
  "inventoryRisk": "12"
}
```

## 🎨 UI Design System

### Color Palette
- **Tabs**: Blue, Green, Purple, Pink, Teal gradients
- **Input**: Indigo border with matching shadow
- **Suggested Questions**: Multi-color borders (Blue, Green, Purple, Pink)
- **Charts**: Consistent typography (11px ticks, 12px legend)

### Transitions
- **Tab Changes**: 300ms fade + blur + scale (98% → 100%)
- **Hover Effects**: Scale, shadow, and gradient enhancements
- **Voice Input**: Animated waveform and progress indicators

### Typography
- **Font**: Inter/system-ui
- **Chart Labels**: 11px ticks, 12px legend/tooltip
- **Headers**: Semibold, varied sizes for hierarchy

## ⚙️ Configuration

### Environment Variables

|| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Workspace hostname | Required |
| `DATABRICKS_SQL_HTTP_PATH` | SQL Warehouse path | Required |
| `DATABRICKS_TOKEN_FOR_SQL` | PAT for SQL access | Required |
| `GENIE_SPACE_ID` | Genie space ID | Required |
| `DATABRICKS_TOKEN_FOR_GENIE` | PAT for Genie | Required |
| `GENIE_CACHE_TTL_SECONDS` | Genie cache TTL | 300 |
| `SQL_CACHE_TTL_SECONDS` | SQL cache TTL | 300 |
| `DASHBOARD_CACHE_TTL_SECONDS` | Dashboard cache TTL | 120 |
| `GENIE_MAX_CONCURRENT` | Max concurrent Genie requests | 1 |
| `SQL_POOL_SIZE` | SQL connection pool size | 3 |
| `LOG_LEVEL` | Logging level | INFO |
| `DATABRICKS_INSECURE` | Disable TLS verification | false |

### Caching Strategy

The backend implements a **three-layer caching system**:

1. **Genie Cache**: Caches Genie API responses (5 min TTL)
   - Thread-safe with locks
   - Semaphore for rate limiting
   - Avoids 429 (Too Many Requests) errors

2. **SQL Cache**: Caches raw SQL query results (5 min TTL)
   - Reduces warehouse load
   - Shared across dashboard endpoints

3. **Dashboard Cache**: Caches processed dashboard payloads (2 min TTL)
   - Fastest response time
   - Per-endpoint granularity

## 🧪 Testing

### Frontend Tests
```bash
# Run all tests
npm test

# Run tests with UI
npm run test:ui

# Run with coverage
npm run test:coverage
```

**Test Coverage**:
- Component rendering (Header, TabNavigation, GovernanceFooter)
- User interactions (tab changes, button clicks)
- Utility functions (formatting, data processing)

### Backend Tests
```bash
# Run Genie parsing tests
pytest ui/backend/tests/test_genie_parsing.py

# Run with coverage
pytest --cov=backend ui/backend/tests/
```

### Build Validation
```bash
# Type checking + production build
npm run build

# Check bundle sizes
npm run build | grep "gzip:"
```

## 🔒 Security

- **TLS Verification**: Enabled by default (set `DATABRICKS_INSECURE=true` only for testing)
- **Error Handling**: Generic error messages to clients, detailed logging for debugging
- **Token Management**: PATs stored in environment variables, never hardcoded
- **Git Safety**: `app.yaml` (with secrets) is git-ignored; `app_git.yaml` uses placeholders

## 📊 Data Refresh

Dashboard data is queried directly from Unity Catalog. To update:

1. **Regenerate mock data**:
   ```bash
   python generate_mock_data.py
   ```

2. **Upload to Databricks**:
   ```bash
   databricks fs cp data/*.csv dbfs:/FileStore/discount-tire-demo/
   ```

3. **Refresh tables**:
   Run the Databricks notebook to reload Delta tables.

4. **Dashboard auto-updates**: Next cache expiry will fetch new data.

## 🗺️ Map View

The map uses **Leaflet** with **OpenStreetMap** tiles. Store coordinates are derived from state centroids unless you add real `latitude`/`longitude` columns to the `stores` table.

**Features**:
- 25 store locations with performance metrics
- Popup cards showing revenue and units sold
- High-level statistics tiles below the map
- Deterministic jitter to separate overlapping markers

## 🎤 Voice Features

### Speech Recognition
- Browser-based Web Speech API
- Real-time transcript with visual feedback
- Animated waveform during listening

### Text-to-Speech
- Natural cadence with adjusted rate (0.9), pitch (1.0), volume (0.9)
- Automatic pauses at punctuation
- Segment-based playback for better flow
- Voice selection (prefers Samantha, Alex, Google voices)

## 🚨 Troubleshooting

### Charts are blank
- Check `DATABRICKS_SQL_HTTP_PATH` is configured
- Verify SQL Warehouse is running
- Check browser console for 503 errors
- Review server logs for SQL failures

### Genie queries fail
- Verify `GENIE_SPACE_ID` is correct
- Check `DATABRICKS_TOKEN_FOR_GENIE` has access
- Ensure Genie space has access to required tables
- Check for 429 (rate limit) errors

### Voice input not working
- Browser must support Web Speech API (Chrome, Edge, Safari)
- Grant microphone permissions
- Check browser console for errors

### User info shows mock data
- Ensure app is deployed to Databricks (not local)
- Check `X-Forwarded-Email` header is present
- Verify Databricks App authentication is enabled

## 📝 License

This demo is provided as-is for demonstration purposes.

## 🤝 Contributing

This is a demo project. For production use, consider:
- Adding comprehensive error boundaries
- Implementing retry logic for API failures
- Adding E2E tests with Playwright/Cypress
- Setting up CI/CD pipelines
- Adding monitoring/observability
- Implementing role-based access control

## 📞 Support

For questions or issues, contact the Databricks Field Engineering team.
