#!/bin/bash

# Multi-Agent Supervisor Databricks App Deployment Script
# This script deploys the Multi-Agent Supervisor app to your Databricks workspace

set -e

echo "🚀 Multi-Agent Supervisor Databricks App Deployment"
echo "=================================================="

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI is not installed. Please install it first:"
    echo "   pip install databricks-cli"
    echo "   databricks configure"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "databricks.yml" ]; then
    echo "❌ Please run this script from the databricks-app-mas directory"
    exit 1
fi

# Check if environment variables are set
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "⚠️  Environment variables not set. Please set:"
    echo "   export DATABRICKS_HOST='your-workspace-url'"
    echo "   export DATABRICKS_TOKEN='your-personal-access-token'"
    echo ""
    echo "Or create a .env file with these values"
    
    # Try to load from .env file
    if [ -f ".env" ]; then
        echo "📁 Loading from .env file..."
        export $(cat .env | grep -v '^#' | xargs)
    else
        echo "❌ No .env file found. Please create one or set environment variables."
        exit 1
    fi
fi

# Validate configuration
echo "🔍 Validating configuration..."
if ! databricks bundle validate; then
    echo "❌ Configuration validation failed. Please check your databricks.yml file."
    exit 1
fi

echo "✅ Configuration is valid"

# Build the bundle
echo "🔨 Building app bundle..."
if ! databricks bundle build; then
    echo "❌ Bundle build failed."
    exit 1
fi

echo "✅ Bundle built successfully"

# Deploy to development target
echo "🚀 Deploying to development environment..."
if ! databricks bundle deploy --target dev; then
    echo "❌ Deployment failed."
    exit 1
fi

echo "✅ Deployment completed successfully!"

# Display deployment information
echo ""
echo "🎉 Multi-Agent Supervisor App Deployed!"
echo "========================================"
echo ""
echo "📱 To access your app:"
echo "   1. Go to your Databricks workspace: $DATABRICKS_HOST"
echo "   2. Navigate to Apps in the left sidebar"
echo "   3. Find 'Multi-Agent Supervisor' and click Launch"
echo ""
echo "🔧 Next steps:"
echo "   1. Configure your Multi-Agent Supervisor endpoint"
echo "   2. Set up agent endpoints and Genie spaces"
echo "   3. Grant proper permissions to users"
echo "   4. Test the app with sample tasks"
echo ""
echo "📚 Documentation:"
echo "   - App README: README.md"
echo "   - Multi-Agent Supervisor: https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor"
echo "   - Databricks Apps: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/"
echo ""
echo "🆘 Need help? Check the troubleshooting section in the README or open an issue."
