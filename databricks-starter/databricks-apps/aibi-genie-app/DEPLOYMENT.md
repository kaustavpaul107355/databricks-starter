# AIBI Genie App - Databricks Deployment Guide

## 📋 Overview

The AIBI Genie App is a sophisticated Streamlit-based analytics application that provides S&P500 data analysis with AI-powered natural language querying capabilities. This guide provides comprehensive deployment instructions for deploying the app to Databricks Apps.

## 🏗️ App Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Streamlit UI  │───▶│  Authentication  │───▶│  Databricks     │
│                 │    │  & Session Mgmt  │    │  Integration    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Three Main Tabs │    │  Environment     │    │   Data Sources  │
│                 │    │  Variables       │    │                 │
│ • Source Data   │    │                  │    │ • Delta Tables  │
│ • AI/BI Dash    │    │ • DB Tokens      │    │ • Genie Space   │
│ • Genie Space   │    │ • URLs & Paths   │    │ • AI/BI Dash    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🎯 Features

- **📊 Source Data Viewer**: Direct access to S&P500 Delta table with data preview
- **🤖 AI/BI Dashboard**: Embedded Databricks dashboard with visualizations  
- **✨ Genie Space Integration**: Natural language querying with conversational AI
- **🔒 Secure Authentication**: OAuth service principal with user context
- **🎨 Modern UI/UX**: Glassmorphism design with responsive layout

## ⚙️ Prerequisites

### System Requirements
- **Python**: 3.8+
- **Databricks CLI**: v0.251.0+
- **Dependencies**: streamlit, databricks-sql-connector, pandas, plotly, numpy

### Authentication Setup
- Databricks workspace access
- Valid personal access token (PAT)
- SQL Warehouse access permissions
- Genie Space access (for natural language features)

### Required Resources
1. **SQL Warehouse**: `4b9b953939869799` (configured in app.yaml)
2. **Delta Table**: `kaustavpaul_demo.SP500.gold_sp500_analytics`
3. **Genie Space**: Configured space for S&P500 data
4. **AI/BI Dashboard**: Embedded dashboard URL

## 🚀 Deployment Process

### Option 1: Automated Deployment (Recommended)

Use the provided deployment script for a fully automated deployment:

```bash
# Navigate to app directory
cd databricks-apps/aibi-genie-app

# Make deployment script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

The script will:
1. ✅ Validate prerequisites and dependencies
2. 🔍 Review application configuration
3. 📦 Prepare deployment files
4. 🚀 Create/update Databricks App
5. 🔗 Deploy code and configure resources
6. ✅ Verify deployment and provide access information

### Option 2: Manual Deployment

If you prefer manual deployment or need to customize the process:

#### Step 1: Validate Configuration
```bash
# Check CLI authentication
databricks current-user me --profile DEFAULT

# Verify app files exist
ls -la app.py app.yaml requirements.txt
```

#### Step 2: Create/Update App
```bash
# Create new app with SQL warehouse resource
databricks apps create aibi-genie-app --profile DEFAULT --json '{
  "name": "aibi-genie-app",
  "resources": [
    {
      "name": "sql-warehouse",
      "sql_warehouse": {
        "id": "4b9b953939869799", 
        "permission": "CAN_USE"
      }
    }
  ]
}'
```

#### Step 3: Upload Code
```bash
# Get current user
CURRENT_USER=$(databricks current-user me --profile DEFAULT | jq -r .userName)

# Sync application files
databricks sync . "/Users/$CURRENT_USER/aibi-genie-app" --profile DEFAULT
```

#### Step 4: Deploy Application
```bash
# Deploy the app
databricks apps deploy aibi-genie-app --source-code-path "/Users/$CURRENT_USER/aibi-genie-app" --profile DEFAULT

# Get app URL
databricks apps get aibi-genie-app --profile DEFAULT
```

## 🔧 Configuration Details

### Environment Variables (app.yaml)
```yaml
env:
- name: "DATABRICKS_WAREHOUSE_ID"
  valueFrom: 'sql-warehouse'                    # Injected from resource
- name: "DATABRICKS_HTTP_PATH"  
  value: "/sql/1.0/warehouses/4b9b953939869799" # SQL Warehouse path
- name: "DATABRICKS_TOKEN"
  value: "YOUR_DATABRICKS_TOKEN_HERE" # Auth token
- name: "GENIE_SPACE_URL"
  value: "https://oregon.cloud.databricks.com/api/2.0/genie/spaces/01f001b1fbc417899f22e8fb835e9e2b"
- name: "AI_BI_Dashboard_URL"
  value: "https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f0028b87a2155595c588ca246bf6b7"
```

### Resource Configuration
- **SQL Warehouse**: Required for Delta table access
- **Authentication**: OAuth service principal + PAT token
- **Permissions**: CAN_USE on SQL warehouse, CAN_QUERY on data

### Data Sources
- **Primary Table**: `kaustavpaul_demo.SP500.gold_sp500_analytics`
- **Genie Space**: S&P500 analytical workspace
- **Dashboard**: Pre-built AI/BI visualizations

## 🔍 Post-Deployment Verification

### 1. App Health Check
```bash
# Check app status
databricks apps get aibi-genie-app --profile DEFAULT

# View app logs  
databricks apps logs aibi-genie-app --profile DEFAULT

# Monitor app metrics
databricks apps list --profile DEFAULT
```

### 2. Functional Testing

1. **Access the App**: Navigate to the provided URL
2. **Test Authentication**: Verify user context displays correctly
3. **Source Data Tab**: Confirm S&P500 data loads properly
4. **AI/BI Dashboard**: Check embedded dashboard renders
5. **Genie Space**: Test natural language queries

### 3. Expected Functionality

- **Tab Navigation**: Smooth transitions between three main tabs
- **Data Display**: First 100 records from S&P500 dataset shown
- **Dashboard Integration**: Iframe loads external AI/BI dashboard
- **Natural Language**: Genie API processes queries and returns results
- **Error Handling**: Graceful error messages for missing config

## 🛠️ Management Commands

### App Lifecycle
```bash
# Start the app
databricks apps start aibi-genie-app --profile DEFAULT

# Stop the app
databricks apps stop aibi-genie-app --profile DEFAULT

# Update the app
databricks apps update aibi-genie-app --profile DEFAULT

# Delete the app
databricks apps delete aibi-genie-app --profile DEFAULT
```

### Monitoring & Debugging
```bash
# Real-time logs
databricks apps logs aibi-genie-app --profile DEFAULT --tail

# Get detailed app info
databricks apps get aibi-genie-app --profile DEFAULT --output json

# List all deployments
databricks apps list-deployments aibi-genie-app --profile DEFAULT
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Authentication Failures
```bash
# Verify profile configuration
databricks auth profiles

# Test connection
databricks current-user me --profile DEFAULT

# Check token validity
databricks workspace list --profile DEFAULT
```

#### 2. Resource Access Issues
- **SQL Warehouse**: Ensure warehouse ID exists and is accessible
- **Delta Table**: Verify table permissions and catalog access
- **Genie Space**: Confirm space exists and user has access

#### 3. App Deployment Failures
```bash
# Check app creation status
databricks apps get aibi-genie-app --profile DEFAULT

# View deployment logs
databricks apps logs aibi-genie-app --profile DEFAULT

# Validate app.yaml configuration
cat app.yaml
```

#### 4. Runtime Errors
- **Missing Dependencies**: Check requirements.txt completeness
- **Environment Variables**: Verify all required env vars are set
- **Network Access**: Ensure workspace can reach external URLs

### Error Resolution

| Error Type | Solution |
|------------|----------|
| `Profile not found` | Run `databricks configure --profile DEFAULT` |
| `SQL warehouse not accessible` | Check warehouse permissions and status |
| `Genie API errors` | Verify space ID and token permissions |
| `Dashboard not loading` | Check iframe URL accessibility |
| `Data not loading` | Verify Delta table exists and permissions |

## 🔐 Security Considerations

### Authentication & Authorization
- **PAT Token**: Stored as environment variable (not hardcoded)
- **User Context**: Extracted from request headers
- **Resource Access**: Controlled via Databricks permissions

### Best Practices
- **Token Rotation**: Regularly rotate PAT tokens
- **Least Privilege**: Grant minimal required permissions
- **Audit Logs**: Monitor app access and usage
- **Environment Isolation**: Use separate tokens for dev/prod

## 📊 Performance Considerations

### Optimization Settings
- **Caching**: Streamlit cache for data operations (30s TTL)
- **Pagination**: Displays first 100 records for performance
- **Async Operations**: Non-blocking UI during API calls

### Monitoring Metrics
- **Response Times**: Monitor Genie API response latency
- **Error Rates**: Track failed queries and authentication issues
- **Resource Usage**: Monitor SQL warehouse utilization

## 🔄 Updates & Maintenance

### Code Updates
1. Make changes to app.py locally
2. Test changes with local Streamlit server
3. Run deployment script to update production app

### Configuration Updates
1. Update app.yaml with new environment variables
2. Redeploy using deployment script
3. Verify new configuration takes effect

### Dependency Updates
1. Update requirements.txt with new versions
2. Test compatibility locally
3. Deploy and monitor for issues

## 📞 Support & Resources

### Documentation
- [Databricks Apps Documentation](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Genie Space API Documentation](https://docs.databricks.com/genie/index.html)

### Support Channels
- **Databricks Support**: For platform-related issues
- **Internal Team**: For app-specific functionality
- **Community Forums**: For general guidance

---

**Deployment Date**: $(date +%Y-%m-%d)  
**App Version**: 1.0.0  
**Databricks Workspace**: e2-demo-field-eng.cloud.databricks.com  
**Deployed by**: kaustav.paul@databricks.com
