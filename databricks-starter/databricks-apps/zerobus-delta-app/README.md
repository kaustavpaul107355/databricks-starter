# Zerobus Delta App

A FastAPI-based Databricks app that processes incoming payloads and prepares them for writing to Delta tables via Zerobus.

## Features

- **FastAPI Web Server**: High-performance async web server with automatic API documentation
- **Payload Processing**: Receives JSON payloads via POST endpoints
- **Data Transformation**: Validates, transforms, and categorizes incoming data
- **Multi-table Support**: Splits payloads into different categories for routing to different Delta tables
- **Error Handling**: Comprehensive error handling with proper HTTP status codes
- **Logging**: Structured logging for monitoring and debugging

## API Endpoints

### Health Check
- **GET** `/health` - Returns service health status

### Payload Processing
- **POST** `/api/v1/process` - Main endpoint for structured payload processing
- **POST** `/api/v1/process-simple` - Simplified endpoint for basic JSON processing

## Payload Structure

### Structured Endpoint (`/api/v1/process`)
```json
{
  "items": [
    {
      "id": "optional-item-id",
      "data": {
        "key1": "value1",
        "key2": "value2"
      },
      "timestamp": "2025-01-01T00:00:00Z",
      "category": "table1"
    }
  ],
  "source": "api",
  "batch_id": "optional-batch-id"
}
```

### Simple Endpoint (`/api/v1/process-simple`)
```json
{
  "any": "json",
  "structure": "works",
  "data": [1, 2, 3]
}
```

## Response Format

### Success Response
```json
{
  "status": "success",
  "message": "Successfully processed N items",
  "batch_id": "batch_20250101_120000_abc123",
  "processed_count": 3,
  "items_processed": [...]
}
```

### Error Response
```json
{
  "status": "error",
  "message": "Error description",
  "error_code": "HTTP_400",
  "details": {
    "path": "/api/v1/process",
    "method": "POST"
  }
}
```

## Data Transformation

The app performs several transformations on incoming data:

1. **ID Generation**: Generates unique IDs for items that don't have them
2. **Timestamp Addition**: Adds processing timestamps
3. **Category Assignment**: Assigns items to categories (which map to Delta tables)
4. **Metadata Enhancement**: Adds processing metadata and batch information
5. **Validation**: Validates payload structure and data types

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the server:
```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

3. Access API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Testing Examples

### Test with curl
```bash
# Health check
curl http://localhost:8000/health

# Process structured payload
curl -X POST http://localhost:8000/api/v1/process \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {
        "data": {"name": "test", "value": 123},
        "category": "users"
      }
    ],
    "source": "test"
  }'

# Process simple payload
curl -X POST http://localhost:8000/api/v1/process-simple \
  -H "Content-Type: application/json" \
  -d '{"test": "data", "number": 42}'
```

## Deployment to Databricks

This app is configured for deployment to Databricks Apps. The `app.yaml` file contains the deployment configuration.

## Next Phase: Zerobus Integration

The next development phase will add:
- Zerobus client integration
- Delta table writing capabilities
- ACK/NACK handling
- Retry logic
- Dead letter queue support

## Architecture

```
Incoming Request → FastAPI → Validation → Transformation → [Zerobus] → Delta Tables
                     ↓
                Error Handling → HTTP Response (200/400)
```
