#!/bin/bash

# Local Development Script for Zerobus Delta App
# Follows Databricks Apps development standards

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🛠️  Zerobus Delta App - Local Development${NC}"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python -m venv venv
fi

# Activate virtual environment
echo -e "${BLUE}Activating virtual environment...${NC}"
source venv/bin/activate

# Install/upgrade dependencies
echo -e "${BLUE}Installing dependencies...${NC}"
pip install --upgrade pip
pip install -r requirements.txt

# Set development environment variables
export APP_ENV=development
export LOG_LEVEL=INFO
export PORT=8000

# Load environment variables if .env file exists
if [ -f ".env" ]; then
    echo -e "${BLUE}Loading environment variables from .env...${NC}"
    set -a  # automatically export all variables
    source .env
    set +a
else
    echo -e "${YELLOW}No .env file found. Using defaults...${NC}"
    echo "Copy env.example to .env and customize if needed."
fi

# Start the application
echo -e "${GREEN}🚀 Starting Zerobus Delta App...${NC}"
echo ""
echo "📍 Local URLs:"
echo "   Web UI:    http://localhost:${PORT:-8000}"
echo "   API Docs:  http://localhost:${PORT:-8000}/docs"
echo "   Health:    http://localhost:${PORT:-8000}/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the application with hot reload
uvicorn app:app --reload --host 0.0.0.0 --port ${PORT:-8000}
