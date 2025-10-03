# Zerobus Delta App

A FastAPI application for processing structured data and writing to Delta tables on Databricks.

## Overview

This application provides a web interface and REST API for submitting structured product data and writing it directly to Delta tables. It uses the Databricks SDK for reliable data writing while maintaining compatibility with Zerobus SDK interfaces.

## Features

- **Web UI**: Interactive interface for testing structured data submission
- **REST API**: Programmatic endpoints for data processing
- **Direct Delta Writing**: Reliable data writing via Databricks SQL warehouse
- **Comprehensive Logging**: Detailed logging and error reporting
- **Multiple Data Schemas**: Support for Products, Users, Orders, and Custom schemas

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web UI        │    │   FastAPI App    │    │  Delta Table    │
│   (Static HTML) │───▶│   (Python)       │───▶│  (Databricks)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Direct Delta     │
                       │ Writer Module    │
                       └──────────────────┘
```

## Core Files

### Application Files
- **`working_app_clean.py`** - Main FastAPI application with all endpoints
- **`direct_delta_writer_clean.py`** - Direct Delta table writer implementation
- **`static/index.html`** - Web UI for testing and interaction

### Configuration Files
- **`app_clean.yaml`** - Databricks Apps runtime configuration
- **`databricks_clean.yml`** - Databricks bundle deployment configuration
- **`requirements_clean.txt`** - Python dependencies

## API Endpoints

### Main Endpoints
- **`GET /`** - Serve web UI
- **`GET /health`** - Health check endpoint
- **`POST /api/v1/process-structured`** - Process structured payload and write to Delta

### Debug Endpoints
- **`GET /debug/logs`** - Get recent application logs
- **`GET /debug/environment`** - Get environment information
- **`GET /debug/delta-writer`** - Check Delta writer status

## Data Schema

### Product Schema (Default)
```json
{
  "schema_type": "products",
  "items": [
    {
      "product_id": "PROD001",
      "product_name": "iPhone 15",
      "product_price": 999.99,
      "category": "electronics",
      "sale_start_date": "2024-01-01",
      "sale_stop_date": "2024-12-31"
    }
  ]
}
```

### Processing Metadata (Added Automatically)
- `record_id` - Unique record identifier
- `processed_at` - Processing timestamp
- `batch_id` - Batch identifier
- `source` - Data source identifier

## Delta Table Configuration

- **Catalog**: `kaustavpaul_demo`
- **Schema**: `zerobus_delta`
- **Table**: `zerobus_products_data`
- **Full Name**: `kaustavpaul_demo.zerobus_delta.zerobus_products_data`

## Dependencies

- **FastAPI 0.109.0** - Web framework
- **Uvicorn 0.27.0** - ASGI server
- **Databricks SDK 0.18.0** - Databricks integration
- **Pandas 2.1.4** - Data processing
- **Requests 2.31.0** - HTTP client

## Local Development

### Prerequisites
- Python 3.9+
- Databricks CLI configured
- Access to Databricks workspace

### Setup
```bash
# Install dependencies
pip install -r requirements_clean.txt

# Run locally
uvicorn working_app_clean:app --host 0.0.0.0 --port 8000
```

### Access
- Web UI: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health

## Deployment to Databricks Apps

### Prerequisites
- Databricks CLI configured with staging workspace
- Profile: `staging-pat` or equivalent

### Deploy
```bash
# Upload source files
databricks workspace import /Workspace/Users/your.email@databricks.com/zerobus-delta-app/working_app_clean.py --file working_app_clean.py --format RAW --profile staging-pat

databricks workspace import /Workspace/Users/your.email@databricks.com/zerobus-delta-app/direct_delta_writer_clean.py --file direct_delta_writer_clean.py --format RAW --profile staging-pat

databricks workspace import /Workspace/Users/your.email@databricks.com/zerobus-delta-app/app_clean.yaml --file app_clean.yaml --format RAW --profile staging-pat

databricks workspace import /Workspace/Users/your.email@databricks.com/zerobus-delta-app/requirements_clean.txt --file requirements_clean.txt --format RAW --profile staging-pat

# Deploy app
DATABRICKS_CONFIG_PROFILE=staging-pat databricks apps deploy zerobus-delta-app --source-code-path /Workspace/Users/your.email@databricks.com/zerobus-delta-app
```

## Configuration

### Environment Variables
- `DATABRICKS_HOST` - Databricks workspace URL
- `DATABRICKS_TOKEN` - Authentication token
- `DATABRICKS_CLIENT_ID` - OAuth client ID (optional)
- `DATABRICKS_CLIENT_SECRET` - OAuth client secret (optional)

### SQL Warehouse
- **Warehouse ID**: `791ba2a31c7fd70a` (Starter Endpoint)
- **Purpose**: Execute SQL INSERT statements for Delta table writing

## Monitoring and Debugging

### Logs
Access logs via the debug endpoint:
```bash
curl https://your-app-url/debug/logs
```

### Health Check
Monitor app health:
```bash
curl https://your-app-url/health
```

### Delta Writer Status
Check Delta writer configuration:
```bash
curl https://your-app-url/debug/delta-writer
```

## Error Handling

The application provides comprehensive error handling:

- **Import Errors**: Graceful fallback when modules are unavailable
- **SQL Errors**: Detailed error reporting with statement IDs
- **Validation Errors**: Clear messages for invalid input data
- **Authentication Errors**: Helpful guidance for token issues

## Security

- **Input Validation**: All inputs validated via Pydantic models
- **SQL Injection Prevention**: Proper SQL escaping and parameterization
- **Authentication**: Uses Databricks workspace authentication
- **HTTPS**: Secure communication in production

## Performance

- **Async Processing**: Non-blocking request handling
- **Batch Processing**: Efficient handling of multiple records
- **Connection Pooling**: Reused Databricks SDK connections
- **Error Recovery**: Graceful handling of partial failures

## Troubleshooting

### Common Issues

1. **"Databricks SDK not available"**
   - Ensure `databricks-sdk` is installed
   - Check Python environment

2. **"WorkspaceClient not initialized"**
   - Verify Databricks authentication
   - Check environment variables

3. **"SQL execution failed"**
   - Verify table exists
   - Check warehouse permissions
   - Review SQL syntax in logs

4. **"Invalid token"**
   - Refresh Databricks token
   - Verify workspace access

### Debug Steps
1. Check `/debug/environment` for configuration
2. Check `/debug/delta-writer` for SDK status
3. Review `/debug/logs` for detailed error messages
4. Verify table schema matches data structure

## Version History

- **v2.0.0** - Clean, documented version with direct Delta writing
- **v1.0.0** - Initial version with Zerobus SDK integration attempts

## Support

For issues and questions:
1. Check debug endpoints for detailed error information
2. Review application logs
3. Verify Databricks workspace connectivity
4. Ensure proper authentication configuration
