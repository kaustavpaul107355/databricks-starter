# AIBI Genie App

**AI-Powered S&P500 Analytics with Natural Language Querying**

A sophisticated Streamlit-based application that provides comprehensive S&P500 data analysis through three integrated interfaces: direct data access, AI/BI dashboards, and natural language querying via Databricks Genie Space.

## 🚀 Quick Start

### Deploy to Databricks Apps
```bash
# Navigate to app directory
cd databricks-apps/aibi-genie-app

# Run automated deployment
./deploy.sh
```

### Run Locally for Development  
```bash
# Start local development server
./run_local.sh
```

## 🎯 Features

### 📊 **Source Data Tab**
- **Direct Delta Table Access**: Live connection to `kaustavpaul_demo.SP500.gold_sp500_analytics`
- **Data Preview**: Interactive table showing first 100 records
- **Real-time Updates**: Fresh data with 30-second caching
- **Responsive Design**: Modern UI with smooth animations

### 🤖 **AI/BI Dashboard Tab**  
- **Embedded Visualizations**: Pre-built analytical dashboards
- **Interactive Charts**: Drill-down capabilities and filtering
- **Performance Metrics**: KPIs and trend analysis
- **Seamless Integration**: Native iframe integration

### ✨ **Genie Space Tab**
- **Natural Language Queries**: Ask questions in plain English
- **Suggested Prompts**: Pre-configured analytical questions
- **Real-time Processing**: Live API integration with response streaming
- **Rich Results**: Tabular data with full API response details

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Streamlit UI  │───▶│  Authentication  │───▶│   Databricks    │
│                 │    │  OAuth + Headers │    │   Integration   │
│ • Tab Navigation│    │                  │    │                 │
│ • Modern Design │    │ • User Context   │    │ • Delta Tables  │
│ • Animations    │    │ • Token Auth     │    │ • Genie API     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📋 Prerequisites

- **Python**: 3.8+
- **Databricks CLI**: v0.251.0+
- **Workspace Access**: e2-demo-field-eng.cloud.databricks.com
- **Permissions**: SQL Warehouse, Delta table, and Genie Space access

## 🔧 Configuration

### Environment Variables
```yaml
DATABRICKS_WAREHOUSE_ID: "4b9b953939869799"    # SQL Warehouse
DATABRICKS_HTTP_PATH: "/sql/1.0/warehouses/..." # Warehouse path  
DATABRICKS_TOKEN: "dapi..."                     # Authentication
GENIE_SPACE_URL: "https://.../genie/spaces/..." # AI querying
AI_BI_Dashboard_URL: "https://.../dashboards/..." # Embedded viz
```

### Data Sources
- **Primary Dataset**: S&P500 historical and analytical data
- **Catalog**: `kaustavpaul_demo`
- **Schema**: `SP500`  
- **Table**: `gold_sp500_analytics`

## 🚀 Deployment Guide

### Option 1: Automated Deployment (Recommended)
The deployment script provides enterprise-grade deployment with validation:

```bash
./deploy.sh
```

**Deployment Phases:**
1. **Prerequisites Validation**: CLI, authentication, dependencies
2. **Configuration Review**: Environment variables, resources
3. **Deployment Process**: App creation, file sync, code deployment  
4. **Verification**: Status check, URL retrieval, functionality test

### Option 2: Manual Deployment
See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed manual deployment instructions.

## 🛠️ Development

### Local Development
```bash
# Start development server
./run_local.sh

# Access at: http://localhost:8501
```

### Project Structure
```
aibi-genie-app/
├── app.py              # Main Streamlit application
├── app.yaml            # Databricks App configuration
├── requirements.txt    # Python dependencies
├── deploy.sh          # Deployment script
├── run_local.sh       # Local development script
├── README.md          # This file
├── DEPLOYMENT.md      # Detailed deployment guide
└── .streamlit/
    └── config.toml    # Streamlit configuration
```

### Dependencies
- **streamlit==1.38.0**: Web application framework
- **databricks-sql-connector**: Delta table access
- **pandas**: Data manipulation
- **plotly==5.24.0**: Interactive visualizations
- **numpy**: Numerical computing
- **requests**: HTTP API calls

## 🔐 Security Features

- **Environment-based Authentication**: PAT token via environment variables
- **OAuth Integration**: Service principal authentication
- **User Context Extraction**: Request header processing
- **Secure API Calls**: Bearer token authentication
- **Error Handling**: Graceful failure with user feedback

## 📊 Usage Examples

### Natural Language Queries
- "What is the average market capitalization of companies in the SP500?"
- "What are the most common sectors represented in the SP500 companies?"
- "What is the monthly average closing price of SP500 over the last year?"
- "Explain the data set"

### Data Exploration
- Browse source data with pagination
- View embedded AI/BI dashboards
- Export query results
- Reset conversation history

## 🔍 Monitoring & Management

### App Management
```bash
# Check status
databricks apps get aibi-genie-app --profile DEFAULT

# View logs
databricks apps logs aibi-genie-app --profile DEFAULT

# Start/Stop
databricks apps start aibi-genie-app --profile DEFAULT
databricks apps stop aibi-genie-app --profile DEFAULT
```

### Health Checks
- **Authentication**: User context display in sidebar
- **Data Access**: Source data table loading
- **Dashboard**: Iframe rendering
- **Genie API**: Query processing and response handling

## 🚨 Troubleshooting

### Common Issues
1. **Authentication Errors**: Check PAT token validity and permissions
2. **Data Loading Issues**: Verify Delta table access and SQL warehouse status
3. **Genie API Failures**: Confirm space access and API endpoints
4. **Dashboard Not Loading**: Check iframe URL and network access

### Debug Commands
```bash
# Test authentication
databricks current-user me --profile DEFAULT

# Check app status
databricks apps get aibi-genie-app --profile DEFAULT --output json

# View detailed logs  
databricks apps logs aibi-genie-app --profile DEFAULT --tail
```

## 📈 Performance

- **Caching**: 30-second TTL for data queries
- **Pagination**: Limited to 100 records for performance
- **Async UI**: Non-blocking operations with loading states
- **Error Resilience**: Retry logic and graceful degradation

## 🤝 Contributing

1. **Local Development**: Use `./run_local.sh` for testing
2. **Code Changes**: Follow existing patterns and security practices
3. **Testing**: Verify all three tabs function correctly
4. **Deployment**: Test with `./deploy.sh` before production

## 📄 License

This application is part of the Databricks workspace deployment and follows enterprise licensing terms.

## 🆘 Support

- **Technical Issues**: Databricks platform support
- **App Functionality**: Internal development team
- **Data Access**: Workspace administrators

---

**Version**: 1.0.0  
**Last Updated**: $(date +%Y-%m-%d)  
**Maintainer**: kaustav.paul@databricks.com  
**Workspace**: e2-demo-field-eng.cloud.databricks.com
