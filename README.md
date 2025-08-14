# Databricks Starter

A starter template for Databricks development projects.

## Overview

This repository contains starter code and configurations for Databricks development, including:
- Notebook templates
- Configuration files
- Utility functions
- Best practices and examples
- **Databricks Connect setup for local development**
- **User folder management and file operations**

## Getting Started

### Prerequisites

- Databricks workspace access
- Python 3.8+
- Required Python packages (see `requirements.txt`)

### Setup

1. Clone this repository:
   ```bash
   git clone git@github.com:kaustavpaul107355/databricks-starter.git
   cd databricks-starter
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your Databricks connection (see `config/` directory)

## 🚀 Databricks Connect Setup

This project includes Databricks Connect configuration for local development, allowing you to develop and test your code locally while using your Databricks cluster's compute resources.

### Quick Setup

1. **Set environment variables**:
   ```bash
   # Copy the template and configure with your values
   cp setup_env.sh setup_env_local.sh
   # Edit setup_env_local.sh with your actual values
   
   # Load the environment variables
   source setup_env_local.sh
   ```

2. **Test the connection**:
   ```bash
   python setup_databricks_connect.py
   ```

3. **Run local development examples**:
   ```bash
   python notebooks/local_development_example.py
   ```

### Token Management

- **Token lifetime**: 90 days maximum (workspace policy)
- **Token type**: Personal Access Token
- **Security**: Tokens are stored in environment variables, not in code
- **Renewal**: Set calendar reminders to renew before expiration

To create a new token:
```bash
databricks tokens create --comment "Databricks Connect Token" --profile your-profile
```

**Note**: The `--lifetime-seconds` parameter may not work as expected due to workspace security policies. Tokens typically default to 90 days maximum.

### Environment Variables Required

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_ORG_ID="your-org-id"
```

## 📁 User Folder Management

This project includes powerful utilities for managing files and assets in your Databricks user folder:

### Features

- **File Operations**: Create, read, write, and delete files
- **Multiple Formats**: Support for Parquet, CSV, JSON, and Delta formats
- **Audit Trail**: Automatic tracking of creation and modification
- **Batch Operations**: Efficient handling of large datasets
- **Error Handling**: Robust error handling and logging

### Quick Demo

```bash
# Run the user folder management demo
python demo_user_folder.py
```

### Usage Examples

```python
from src.user_folder_manager import create_user_folder_manager
from databricks.connect import DatabricksSession

# Create a Spark session
spark = DatabricksSession.builder.remote().getOrCreate()

# Initialize user folder manager
user_manager = create_user_folder_manager(spark)

# List files in your user folder
files = user_manager.list_files()

# Create test data
test_data = [("file1", "content1"), ("file2", "content2")]
user_manager.create_test_data("test.parquet", test_data, ["name", "content"])

# Read files
df = user_manager.read_file("test.parquet")

# Write in different formats
user_manager.write_file(df, "output.csv", "csv")
user_manager.write_file(df, "output.json", "json")
```

## Project Structure

```
databricks-starter/
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── setup_databricks_connect.py  # Connection test script
├── demo_user_folder.py          # User folder management demo
├── setup_env.sh                 # Environment template
├── .gitignore                   # Git ignore patterns
├── config/                      # Configuration files
├── notebooks/                   # Databricks notebooks
│   ├── 01_getting_started.py   # Basic Databricks notebook
│   ├── local_development_example.py # Local dev example
│   └── user_folder_explorer.py # User folder exploration
├── src/                         # Source code
│   ├── databricks_utils.py     # Core utilities
│   └── user_folder_manager.py  # User folder management
└── tests/                       # Test files
```

## 🔧 Local Development Workflow

1. **Start your Databricks cluster** in the workspace
2. **Set environment variables** using `setup_env_local.sh`
3. **Develop locally** using your IDE with full Databricks functionality
4. **Test your code** before deploying to the workspace
5. **Use version control** for your development workflow
6. **Manage user folder assets** programmatically

## 🧪 Testing

The test suite is configured to work with Databricks Connect:
```bash
# Run tests (requires Databricks Connect environment variables)
pytest tests/ -v

# Tests will be skipped if Databricks Connect is not configured
```

## 🎯 Use Cases

This starter template is perfect for:

- **Data Engineers**: Building ETL pipelines and data transformations
- **Data Scientists**: Developing ML models and data analysis workflows
- **Analysts**: Creating data processing scripts and reports
- **DevOps Engineers**: Automating Databricks operations and deployments
- **Teams**: Collaborative development with version control and testing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

- **Author**: Kaustav Paul
- **Email**: kaustav.paul@databricks.com
- **GitHub**: [@kaustavpaul107355](https://github.com/kaustavpaul107355)
