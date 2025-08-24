#!/bin/bash

# Local Development Script for Multi-Agent Chat App
# This script runs the chat app locally for development and testing

set -e

echo "🔧 Setting up local development environment..."

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found!"
    echo "📝 Please copy env.example to .env and configure your settings:"
    echo "   cp env.example .env"
    echo "   # Then edit .env with your actual values"
    echo ""
    echo "Required environment variables:"
    echo "  - DATABRICKS_HOST"
    echo "  - DATABRICKS_TOKEN"
    echo "  - SERVING_ENDPOINT (defaults to mas-6c04fa76-endpoint)"
    echo ""
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Set default endpoint if not provided
if [ -z "$SERVING_ENDPOINT" ]; then
    export SERVING_ENDPOINT="mas-6c04fa76-endpoint"
    echo "🔗 Using default endpoint: $SERVING_ENDPOINT"
fi

echo "🚀 Starting Streamlit app..."
echo "📱 The app will open in your browser at http://localhost:8501"
echo "🔄 Press Ctrl+C to stop the app"
echo ""

# Run the Streamlit app
streamlit run app.py --server.port 8501 --server.address localhost
