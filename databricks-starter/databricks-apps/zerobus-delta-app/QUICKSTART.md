# Zerobus Delta App - Quick Start Guide

A production-ready FastAPI application for processing payloads and preparing them for Delta table writes via Zerobus, following Databricks Apps enterprise standards.

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Databricks CLI installed and configured
- Access to a Databricks workspace
- Personal Access Token for your workspace

### 1. Local Development

```bash
# Clone and navigate to the app directory
cd zerobus-delta-app

# Set up local development (creates venv, installs deps)
./run_local.sh
```

This will start the app at http://localhost:8000 with:
- **Web UI**: Interactive testing interface
- **API Docs**: Swagger documentation at `/docs`
- **Health Check**: Status monitoring at `/health`

### 2. Deploy to Databricks

```bash
# Deploy using the automated script
./deploy.sh
```

The deployment script will:
- ✅ Validate prerequisites
- ✅ Create/update the Databricks App
- ✅ Deploy your code
- ✅ Provide the app URL

## 📋 Features

### ✨ Enterprise-Ready
- **Production logging** with configurable levels
- **Environment-based configuration** (dev/prod)
- **Security middleware** with CORS protection
- **Error handling** with detailed logging
- **Health monitoring** with dependency checks

### 🎯 Core Functionality
- **POST /api/v1/process**: Structured payload processing with validation
- **POST /api/v1/process-simple**: Simple JSON processing
- **GET /health**: Comprehensive health checks
- **Web UI**: Interactive testing interface

### 🔧 Data Processing
- **Payload validation** with Pydantic models
- **Category-based routing** for Delta table preparation
- **Batch processing** with unique IDs and metadata
- **Performance monitoring** with timing metrics

## 🌐 Web Interface

The app includes a beautiful web UI for testing:

1. **Request Builder**: Select endpoints and build payloads
2. **Template System**: Pre-built examples for quick testing
3. **Real-time Results**: Instant response display with formatting
4. **Request History**: Track your testing with performance metrics
5. **Status Monitoring**: Visual indicators for success/error states

## 📊 API Endpoints

### Main Processing Endpoint
```bash
POST /api/v1/process
Content-Type: application/json

{
  "items": [
    {
      "data": {"name": "Alice", "email": "alice@example.com"},
      "category": "users"
    },
    {
      "data": {"order_id": "12345", "amount": 99.99},
      "category": "orders"
    }
  ],
  "source": "api_client",
  "batch_id": "batch_001"
}
```

### Simple Processing Endpoint
```bash
POST /api/v1/process-simple
Content-Type: application/json

{
  "message": "Hello World",
  "data": {"test": true}
}
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file from the template:
```bash
cp env.example .env
```

Key variables:
- `APP_ENV`: `development` or `production`
- `LOG_LEVEL`: `DEBUG`, `INFO`, `WARNING`, `ERROR`
- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: Your personal access token

### Production Settings

For production deployment, the app automatically:
- Disables debug endpoints
- Restricts CORS to Databricks domains
- Enables enhanced security logging
- Optimizes error responses

## 🧪 Testing

### Local Testing
```bash
# Start the app
./run_local.sh

# In another terminal, test endpoints
curl http://localhost:8000/health
curl -X POST http://localhost:8000/api/v1/process-simple \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'
```

### Web UI Testing
1. Open http://localhost:8000 in your browser
2. Select an endpoint from the dropdown
3. Use template buttons to load example payloads
4. Click "Send Request" to test
5. View results and request history

## 📈 Monitoring

### Health Checks
```bash
# Basic health
GET /health

# API information
GET /api/info

# Debug info (development only)
GET /debug
```

### Logging
The app provides structured logging:
- Request/response logging
- Performance metrics
- Error tracking with stack traces
- Category distribution monitoring

## 🔄 Next Phase: Zerobus Integration

The app is architected for easy Zerobus integration:

1. **Categorized Data**: Items are already grouped by category for table routing
2. **Batch Processing**: Batch IDs and metadata are ready for Zerobus
3. **Error Handling**: ACK/NACK response handling is prepared
4. **Monitoring**: Performance and status tracking is built-in

## 🚨 Troubleshooting

### Common Issues

**App won't start locally:**
```bash
# Check Python version
python --version  # Should be 3.9+

# Reinstall dependencies
rm -rf venv
./run_local.sh
```

**Deployment fails:**
```bash
# Check Databricks CLI
databricks auth profiles

# Validate configuration
databricks bundle validate --profile DEFAULT
```

**Web UI not loading:**
- Ensure the `static/` directory exists
- Check browser console for errors
- Verify CORS settings for your domain

### Getting Help

1. Check the logs: `tail -f app.log`
2. Verify environment variables: `env | grep APP_`
3. Test endpoints individually with curl
4. Review the deployment output for errors

## 📚 Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web UI        │    │   FastAPI        │    │   Future:       │
│   (Static)      │───▶│   Processing     │───▶│   Zerobus       │
│                 │    │   Engine         │    │   Integration   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Delta Tables   │
                       │   (Categories)   │
                       └──────────────────┘
```

## 🎉 Success!

Your Zerobus Delta App is now ready for enterprise use with:
- ✅ Production-ready FastAPI server
- ✅ Interactive web testing interface
- ✅ Enterprise security and logging
- ✅ Automated deployment pipeline
- ✅ Comprehensive monitoring
- ✅ Ready for Zerobus integration

Visit your deployed app URL to start processing payloads! 🚀
