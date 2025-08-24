#!/bin/bash

# Multi-Agent Supervisor Chat App Setup Script
# Following the official Databricks app template pattern

set -e

echo "🔧 Setting up Multi-Agent Supervisor Chat App..."
echo "================================================"

# Check if Python 3.9+ is available
echo "🐍 Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
if [ -z "$PYTHON_VERSION" ]; then
    echo "❌ Python 3 is not installed. Please install Python 3.9 or higher."
    exit 1
fi

PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 9 ]); then
    echo "❌ Python $PYTHON_VERSION is not supported. Please install Python 3.9 or higher."
    exit 1
fi

echo "✅ Python $PYTHON_VERSION is supported"

# Check if pip is available
echo "📦 Checking pip..."
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not installed. Please install pip3."
    exit 1
fi
echo "✅ pip3 is available"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "🔧 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"

# Install dependencies
echo "📥 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Check if Databricks CLI is installed
echo "🔍 Checking Databricks CLI..."
if ! command -v databricks &> /dev/null; then
    echo "📦 Installing Databricks CLI..."
    pip install databricks-cli
    echo "✅ Databricks CLI installed"
else
    echo "✅ Databricks CLI is already installed"
fi

# Check if jq is installed (for JSON parsing)
echo "🔍 Checking jq..."
if ! command -v jq &> /dev/null; then
    echo "⚠️  jq is not installed. It's recommended for JSON parsing in deployment scripts."
    echo "   On macOS: brew install jq"
    echo "   On Ubuntu/Debian: sudo apt-get install jq"
    echo "   On CentOS/RHEL: sudo yum install jq"
else
    echo "✅ jq is available"
fi

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# Multi-Agent Supervisor Chat App Environment Configuration
# Copy this file to .env and fill in your actual values

# Databricks Workspace Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token

# Multi-Agent Supervisor Endpoint Configuration
SERVING_ENDPOINT=mas-6c04fa76-endpoint
WORKSPACE_ID=your-workspace-id

# Optional: Additional Configuration
# DATABRICKS_CLUSTER_ID=your-cluster-id
# DATABRICKS_ORG_ID=your-org-id
EOF
    echo "✅ .env file created"
    echo "⚠️  Please edit .env file with your actual Databricks configuration"
else
    echo "✅ .env file already exists"
fi

# Check Databricks CLI configuration
echo "🔐 Checking Databricks CLI configuration..."
if ! databricks current-user me &> /dev/null; then
    echo "⚠️  Databricks CLI is not configured. Please run:"
    echo "   databricks configure"
    echo ""
    echo "You'll need:"
    echo "   - Databricks host URL (e.g., https://your-workspace.cloud.databricks.com)"
    echo "   - Personal access token"
else
    echo "✅ Databricks CLI is configured"
    echo "👤 Current user: $(databricks current-user me | jq -r .userName)"
fi

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env file with your Databricks configuration"
echo "2. Configure Databricks CLI if not already done: databricks configure"
echo "3. Test locally: streamlit run app.py"
echo "4. Deploy to Databricks: ./deploy.sh"
echo ""
echo "🔗 Useful links:"
echo "   - Databricks Apps: https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app"
echo "   - Multi-Agent Supervisor: https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor"
echo "   - Databricks CLI: https://docs.databricks.com/dev-tools/cli/index.html"
echo ""
echo "🚀 Happy coding!"
