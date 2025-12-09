#!/bin/bash

# AIBI Genie App - Local Development Server
# This script sets up and runs the app locally for development and testing

set -e

echo "🏠 Starting AIBI Genie App Locally"
echo "=================================="

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if we're in the right directory
if [[ ! -f "app.py" ]]; then
    print_error "app.py not found. Please run this script from the aibi-genie-app directory."
    exit 1
fi

# Set up environment variables for local development
print_info "Setting up local environment variables..."

# Load environment variables from app.yaml values for local testing
export DATABRICKS_HOST="e2-demo-field-eng.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/4b9b953939869799"
export DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN_HERE"
export DATABRICKS_TOKEN_FOR_GENIE="YOUR_DATABRICKS_TOKEN_HERE"
export GENIE_SPACE_URL="https://e2-demo-field-eng.cloud.databricks.com/api/2.0/genie/spaces/01f050501b7912148a8ee89a422369d6"
export AI_BI_Dashboard_URL="https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f0028b87a2155595c588ca246bf6b7"

# Simulate Databricks warehouse ID (this would normally be injected by the platform)
export DATABRICKS_WAREHOUSE_ID="4b9b953939869799"

print_status "Environment variables configured"

# Check Python dependencies
print_info "Checking Python dependencies..."

# Check if required packages are available
REQUIRED_PACKAGES=("streamlit" "databricks" "pandas" "plotly" "numpy" "requests")
MISSING_PACKAGES=()

for package in "${REQUIRED_PACKAGES[@]}"; do
    if ! python -c "import $package" 2>/dev/null; then
        MISSING_PACKAGES+=($package)
    fi
done

if [[ ${#MISSING_PACKAGES[@]} -gt 0 ]]; then
    print_warning "Missing packages detected: ${MISSING_PACKAGES[*]}"
    print_info "Installing missing dependencies..."
    
    # Install from requirements.txt
    if [[ -f "requirements.txt" ]]; then
        pip install -r requirements.txt
        print_status "Dependencies installed from requirements.txt"
    else
        print_error "requirements.txt not found. Please install dependencies manually."
        exit 1
    fi
else
    print_status "All required dependencies are available"
fi

# Check Databricks connectivity (optional test)
print_info "Testing Databricks connectivity..."
if python -c "
import os
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

def test_connection():
    try:
        config = Config(
            host=f'https://{os.getenv(\"DATABRICKS_HOST\")}',
            client_id=os.getenv('DATABRICKS_CLIENT_ID'),  # Will be None in local mode
            client_secret=os.getenv('DATABRICKS_CLIENT_SECRET'))  # Will be None in local mode
        
        # For local testing, we'll just verify the environment variables are set
        required_vars = ['DATABRICKS_HOST', 'DATABRICKS_HTTP_PATH', 'DATABRICKS_TOKEN']
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            print(f'Missing environment variables: {missing}')
            return False
            
        print('Environment configuration valid')
        return True
    except Exception as e:
        print(f'Configuration test failed: {e}')
        return False

test_connection()
" 2>/dev/null; then
    print_status "Databricks configuration validated"
else
    print_warning "Databricks connectivity test failed (this may be expected in local mode)"
fi

# Display configuration summary
echo ""
print_info "Local Development Configuration:"
echo "  • Host: $DATABRICKS_HOST"
echo "  • HTTP Path: $DATABRICKS_HTTP_PATH"
echo "  • Token: ${DATABRICKS_TOKEN:0:8}...${DATABRICKS_TOKEN: -8}"
echo "  • Warehouse ID: $DATABRICKS_WAREHOUSE_ID"
echo ""

# Check if Streamlit is available
if ! command -v streamlit &> /dev/null; then
    print_error "Streamlit command not found. Please install streamlit:"
    echo "  pip install streamlit"
    exit 1
fi

# Start Streamlit server
print_info "Starting Streamlit development server..."
echo ""
echo "🌐 The app will be available at: http://localhost:8501"
echo "📝 Press Ctrl+C to stop the server"
echo ""

# Set Streamlit configuration for development
export STREAMLIT_SERVER_PORT=8501
export STREAMLIT_SERVER_ADDRESS=0.0.0.0
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
export STREAMLIT_SERVER_HEADLESS=true

# Run the app
streamlit run app.py --server.port 8501 --server.address 0.0.0.0

print_info "Server stopped"
