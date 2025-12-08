# Databricks Direct Write App

A comprehensive FastAPI application for processing structured data and writing to Delta tables using multiple high-performance writer implementations.

## 🎯 Overview

This application provides a production-ready solution for ingesting structured data into Databricks Delta tables with support for multiple writing strategies:

- **🚀 Zerobus Writer**: High-performance streaming via Zerobus Direct Write API
- **🏗️ Direct Delta Writer**: SQL-based writing via Databricks SDK  
- **🧪 Mock Writer**: Testing and development fallback

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Web UI        │    │   FastAPI App    │    │   Writer System     │
│   (index.html)  │───▶│   (main.py)      │───▶│   (writers/)        │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                │                         │
                                ▼                         ▼
                       ┌──────────────────┐    ┌─────────────────────┐
                       │   Data Models    │    │   Delta Tables      │
                       │   (Pydantic)     │    │   (Databricks)      │
                       └──────────────────┘    └─────────────────────┘
```

### Key Components

- **FastAPI Application** (`main.py`): Core web server with REST API endpoints
- **Writer System** (`writers/`): Modular data writing implementations
- **Web Interface** (`static/index.html`): Interactive UI for data submission
- **Configuration** (`app.yaml`, `databricks.yml`): Deployment and runtime configuration
- **SQL Scripts** (`sql/`): Database setup and maintenance scripts
- **Documentation** (`docs/`): API references and implementation guides

## 🚀 Features

### Production Features
- ✅ **Multi-Writer Architecture**: Choose between Zerobus, Direct Delta, or Mock writers
- ✅ **Comprehensive Logging**: Structured logging with performance metrics
- ✅ **Error Handling**: Detailed error reporting with context and troubleshooting
- ✅ **Data Validation**: Pydantic models with business rule validation
- ✅ **Source Tracking**: Complete data lineage with writer method tracking
- ✅ **Performance Monitoring**: Processing time and throughput metrics

### Web Interface Features
- 🌐 **Interactive Forms**: Easy data entry with validation feedback
- 🔧 **Writer Selection**: Runtime selection of data writing strategy
- 📊 **Real-time Status**: Live feedback on processing results
- 🧹 **Form Management**: Clear, reset, and bulk operations

### Developer Features
- 🔧 **Modular Design**: Easy to extend with new writer implementations
- 🧪 **Testing Support**: Mock writer for safe development and testing
- 📚 **Comprehensive Documentation**: Detailed code comments and API docs
- 🛠️ **Debug Endpoints**: Built-in debugging and status monitoring

## 📁 Project Structure

```
databricks-direct-write-app/
├── 📄 main.py                    # Main FastAPI application
├── 📄 app.yaml                   # Databricks app configuration
├── 📄 databricks.yml             # Asset bundle configuration
├── 📄 requirements.txt           # Python dependencies
├── 📄 README.md                  # This file
├── 📁 static/                    # Web UI assets
│   └── index.html               # Main web interface
├── 📁 writers/                   # Data writer implementations
│   ├── __init__.py              # Package initialization
│   ├── base.py                  # Abstract base classes
│   ├── direct_delta.py          # Direct Delta writer
│   ├── zerobus.py               # Zerobus writer
│   └── factory.py               # Writer factory
├── 📁 sql/                       # Database scripts
│   ├── README.md                # SQL documentation
│   ├── grant_permissions.sql    # Permission setup
│   ├── simple_table_fix.sql     # Table maintenance
│   └── *.sql                    # Other SQL utilities
├── 📁 docs/                      # Documentation
│   ├── README.md                # Documentation index
│   ├── *.pdf                    # API references
│   └── zerobus_reference.txt    # Implementation notes
├── 📁 zerobus_sdk/              # Zerobus SDK
│   ├── aio/                     # Async SDK
│   ├── shared/                  # Shared utilities
│   └── sync/                    # Sync SDK
├── 📄 product_record.proto       # Protobuf schema
└── 📄 product_record_pb2.py      # Generated protobuf classes
```

## 🛠️ Setup and Installation

### Prerequisites

- Python 3.8+
- Databricks workspace access
- Databricks CLI configured
- SQL warehouse access

### Local Development

1. **Clone and navigate to the project**:
   ```bash
   cd databricks-starter/databricks-apps/zerobus-delta-app
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run locally**:
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

4. **Access the application**:
   - Web UI: http://localhost:8000
   - API docs: http://localhost:8000/docs

### Databricks Deployment

1. **Configure Databricks CLI**:
   ```bash
   databricks configure
   ```

2. **Deploy the application**:
   ```bash
   databricks apps deploy zerobus-delta-app --source-code-path /Workspace/Users/your-email/zerobus-delta-app
   ```

3. **Access the deployed app**:
   - The deployment will provide a URL for your app

## 🔧 Configuration

### Writer Configuration

The application supports multiple data writers that can be selected at runtime:

#### Zerobus Writer (Default)
- **Purpose**: High-performance streaming to Delta tables
- **Features**: Protobuf serialization, OAuth2 authentication, automatic reconnection
- **Configuration**: Service Principal credentials required

#### Direct Delta Writer
- **Purpose**: SQL-based writing via Databricks SDK
- **Features**: Immediate feedback, reliable execution, SQL warehouse integration
- **Configuration**: SQL warehouse ID and Databricks SDK authentication

#### Mock Writer
- **Purpose**: Testing and development
- **Features**: Safe simulation, no data persistence, always available
- **Configuration**: No configuration required

### Environment Variables

- `ENABLE_ZEROBUS_WRITER`: Enable/disable Zerobus writer (default: true)
- `ENABLE_DIRECT_DELTA_WRITER`: Enable/disable Direct Delta writer (default: true)
- `DATABRICKS_CLIENT_ID`: Service Principal client ID for Zerobus
- `DATABRICKS_CLIENT_SECRET`: Service Principal client secret for Zerobus

## 📊 Usage

### Web Interface

1. **Open the application** in your browser
2. **Select a writer** from the dropdown (Zerobus, Direct Delta, or Mock)
3. **Fill in product information**:
   - Product ID (e.g., "PROD001")
   - Product Name (e.g., "iPhone 15")
   - Product Price (e.g., 999.99)
   - Category (electronics, general, clothing, books, home)
   - Sale dates (YYYY-MM-DD format)
4. **Click "Process Products Data"** to submit
5. **Review the results** in the response area

### REST API

#### Process Structured Data
```bash
POST /api/v1/process-structured
Content-Type: application/json

{
  "schema_type": "products",
  "writer_type": "zerobus",
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

#### Debug Endpoints
- `GET /debug/zerobus-availability` - Check Zerobus writer status
- `GET /debug/direct-delta-availability` - Check Direct Delta writer status
- `POST /debug/test-direct-delta` - Test Direct Delta writer functionality

## 📈 Monitoring and Logging

### Structured Logging

The application provides comprehensive structured logging:

```
================================================================================
🚀 NEW REQUEST STARTED - Batch ID: abc123...
📊 Request Details:
   - Items Count: 1
   - Schema Type: 'products'
   - Writer Type: 'zerobus' (user requested)
   - Timestamp: 2025-10-03T19:32:00.123456
📋 Items Preview:
   - Item 1: PROD001 - iPhone 15 ($999.99)
================================================================================
```

### Performance Metrics

Each request includes detailed performance information:
- Processing time in milliseconds
- Throughput (items per second)
- Writer-specific metrics
- Success/failure rates

### Error Handling

Comprehensive error reporting with:
- Detailed error messages
- Stack traces for debugging
- Context information
- Suggested remediation steps

## 🗃️ Database Setup

### Table Creation

Use the provided SQL scripts to set up your Delta tables:

```sql
-- Create a Zerobus-compatible table
CREATE TABLE kaustavpaul_demo.zerobus_delta.zerobus_products (
    record_id STRING,
    product_id STRING,
    product_name STRING,
    product_price DOUBLE,
    category STRING,
    sale_start_date STRING,
    sale_stop_date STRING,
    processed_at STRING,
    batch_id STRING,
    source STRING
) USING DELTA;
```

### Permissions

Grant necessary permissions to the Service Principal:

```sql
-- Grant permissions for Zerobus integration
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `your-service-principal-id`;
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `your-service-principal-id`;
GRANT MODIFY ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products TO `your-service-principal-id`;
GRANT SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products TO `your-service-principal-id`;
```

## 🔍 Troubleshooting

### Common Issues

1. **Zerobus Authentication Errors**
   - Verify Service Principal credentials
   - Check table permissions
   - Ensure table compatibility (no unsupported features)

2. **Direct Delta Writer Timeouts**
   - Check SQL warehouse availability
   - Verify warehouse permissions
   - Consider increasing timeout values

3. **Table Compatibility Issues**
   - Use provided SQL scripts to create compatible tables
   - Disable advanced Delta features (row tracking, domain metadata)

### Debug Resources

- Check the `docs/` directory for detailed API documentation
- Use debug endpoints for real-time status checking
- Review application logs for detailed error information
- Consult `sql/README.md` for database setup guidance

## 🤝 Contributing

This application follows Databricks Apps best practices:

1. **Code Organization**: Modular design with clear separation of concerns
2. **Documentation**: Comprehensive inline and external documentation
3. **Error Handling**: Robust error handling with detailed context
4. **Testing**: Mock writer for safe development and testing
5. **Logging**: Structured logging for production monitoring

## 📝 License

This project is part of the Databricks ecosystem and follows Databricks licensing terms.

## 🏷️ Version History

- **v2.0.0** (2025-10-03): Production release with multi-writer architecture
- **v1.0.0** (2025-10-02): Initial release with basic functionality

---

For detailed API documentation, see the `docs/` directory or visit `/docs` when running the application.