#!/bin/bash

# Multi-Agent Supervisor Databricks App Setup Script
# This script helps you configure the deployment environment

set -e

echo "🔧 Multi-Agent Supervisor Databricks App Setup"
echo "=============================================="

# Check if .env file exists
if [ -f ".env" ]; then
    echo "📁 Found existing .env file"
    echo "   Current configuration:"
    cat .env | grep -v "^#" | grep -v "^$" || echo "   (No configuration found)"
    echo ""
    
    read -p "Do you want to overwrite the existing .env file? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "✅ Keeping existing .env file"
        exit 0
    fi
fi

echo "📝 Setting up environment configuration..."

# Get Databricks workspace URL
echo ""
echo "🌐 Databricks Workspace Configuration"
echo "------------------------------------"

read -p "Enter your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com): " DATABRICKS_HOST

if [ -z "$DATABRICKS_HOST" ]; then
    echo "❌ Databricks workspace URL is required"
    exit 1
fi

# Validate URL format
if [[ ! "$DATABRICKS_HOST" =~ ^https://.*\.cloud\.databricks\.com$ ]]; then
    echo "⚠️  Warning: URL doesn't match expected format (https://*.cloud.databricks.com)"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Get Personal Access Token
echo ""
echo "🔑 Personal Access Token"
echo "------------------------"

read -p "Enter your Databricks Personal Access Token: " DATABRICKS_TOKEN

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "❌ Personal Access Token is required"
    echo ""
    echo "💡 To create a token:"
    echo "   1. Go to your Databricks workspace"
    echo "   2. Click on your user icon → User Settings"
    echo "   3. Go to Access Tokens → Generate New Token"
    echo "   4. Copy the token and paste it here"
    exit 1
fi

# Get Multi-Agent Supervisor endpoint name
echo ""
echo "🤖 Multi-Agent Supervisor Endpoint"
echo "----------------------------------"

read -p "Enter your Multi-Agent Supervisor endpoint name (default: mas-6c04fa76-endpoint): " MAS_ENDPOINT_NAME

if [ -z "$MAS_ENDPOINT_NAME" ]; then
    MAS_ENDPOINT_NAME="mas-6c04fa76-endpoint"
fi

# Create .env file
echo ""
echo "📝 Creating .env file..."

cat > .env << EOF
# Multi-Agent Supervisor Databricks App Environment Configuration
# Generated on $(date)

# Databricks Workspace Configuration
DATABRICKS_HOST=$DATABRICKS_HOST
DATABRICKS_TOKEN=$DATABRICKS_TOKEN

# Multi-Agent Supervisor Endpoint Configuration
MAS_ENDPOINT_NAME=$MAS_ENDPOINT_NAME
EOF

echo "✅ .env file created successfully!"

# Test configuration
echo ""
echo "🧪 Testing configuration..."

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Test Databricks CLI connection
echo "   Testing Databricks CLI connection..."
if databricks auth describe --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" > /dev/null 2>&1; then
    echo "   ✅ Databricks CLI connection successful"
else
    echo "   ❌ Databricks CLI connection failed"
    echo "   Please check your workspace URL and token"
    exit 1
fi

# Validate bundle configuration
echo "   Validating bundle configuration..."
if databricks bundle validate > /dev/null 2>&1; then
    echo "   ✅ Bundle configuration is valid"
else
    echo "   ❌ Bundle validation failed"
    echo "   Please check your databricks.yml file"
    exit 1
fi

echo ""
echo "🎉 Setup completed successfully!"
echo "================================"
echo ""
echo "📁 Configuration files created:"
echo "   - .env (environment variables)"
echo "   - databricks.yml (bundle configuration)"
echo ""
echo "🚀 Ready to deploy! Run:"
echo "   ./deploy.sh"
echo ""
echo "💡 Next steps:"
echo "   1. Ensure Multi-Agent Supervisor is configured in your workspace"
echo "   2. Set up agent endpoints and Genie spaces"
echo "   3. Grant proper permissions to users"
echo "   4. Deploy the app using ./deploy.sh"
echo ""
echo "📚 Documentation:"
echo "   - README.md for detailed setup instructions"
echo "   - Multi-Agent Supervisor: https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor"
echo "   - Databricks Apps: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/"
