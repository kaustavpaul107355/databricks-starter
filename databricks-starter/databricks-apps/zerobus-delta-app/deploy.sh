#!/bin/bash

# Databricks Apps Deployment Script
# Follows enterprise deployment standards for Zerobus Delta App

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Databricks Apps Deployment - Zerobus Delta App${NC}"
echo "=================================================="

# Configuration
APP_NAME="zerobus-delta-app"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
WORKSPACE_PATH="/Workspace/Users/\${workspace.current_user.userName}/${APP_NAME}"

# Function to print colored output
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

# Validate prerequisites
print_info "Checking prerequisites..."

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed. Please install it first:"
    echo "  pip install databricks-cli"
    exit 1
fi

# Check if profile exists
if ! databricks auth profiles | grep -q "$DATABRICKS_PROFILE"; then
    print_error "Databricks profile '$DATABRICKS_PROFILE' not found."
    echo "Please configure authentication:"
    echo "  databricks configure --profile $DATABRICKS_PROFILE"
    exit 1
fi

print_status "Prerequisites validated"

# Validate bundle configuration
print_info "Validating bundle configuration..."
if ! databricks bundle validate --profile "$DATABRICKS_PROFILE"; then
    print_error "Bundle validation failed. Please check your databricks.yml configuration."
    exit 1
fi
print_status "Bundle configuration validated"

# Check if app already exists
print_info "Checking for existing app..."
if databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE" &>/dev/null; then
    print_warning "App '$APP_NAME' already exists. Updating existing app..."
    DEPLOY_MODE="update"
else
    print_info "Creating new app '$APP_NAME'..."
    DEPLOY_MODE="create"
fi

# Create clean deployment directory
print_info "Preparing deployment files..."
DEPLOY_DIR="../${APP_NAME}-deploy"
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"

# Copy essential files only (exclude development files)
cp app.py "$DEPLOY_DIR/"
cp app.yaml "$DEPLOY_DIR/"
cp requirements.txt "$DEPLOY_DIR/"
cp README.md "$DEPLOY_DIR/"
cp -r static "$DEPLOY_DIR/" 2>/dev/null || print_warning "Static directory not found, skipping..."

print_status "Deployment files prepared"

# Deploy to workspace
print_info "Uploading files to Databricks workspace..."
databricks workspace import-dir "$DEPLOY_DIR" "$WORKSPACE_PATH" --profile "$DATABRICKS_PROFILE" --overwrite

print_status "Files uploaded to workspace"

# Deploy or update the app
if [ "$DEPLOY_MODE" = "create" ]; then
    print_info "Creating new Databricks App..."
    
    # Create the app
    databricks apps create "$APP_NAME" --profile "$DATABRICKS_PROFILE"
    print_status "App created successfully"
    
    # Deploy the code
    print_info "Deploying application code..."
    databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH" --profile "$DATABRICKS_PROFILE"
    
else
    print_info "Updating existing Databricks App..."
    
    # Deploy the updated code
    databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH" --profile "$DATABRICKS_PROFILE"
fi

print_status "Application deployed successfully"

# Get app information
print_info "Retrieving app information..."
APP_INFO=$(databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE" --output json)
APP_URL=$(echo "$APP_INFO" | python -c "import sys, json; print(json.load(sys.stdin)['url'])" 2>/dev/null || echo "URL not available")
APP_STATUS=$(echo "$APP_INFO" | python -c "import sys, json; print(json.load(sys.stdin)['app_status']['state'])" 2>/dev/null || echo "Status unknown")

# Clean up deployment directory
rm -rf "$DEPLOY_DIR"
print_status "Cleanup completed"

# Display deployment summary
echo ""
echo "🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!"
echo "====================================="
echo -e "App Name:     ${GREEN}$APP_NAME${NC}"
echo -e "Status:       ${GREEN}$APP_STATUS${NC}"
echo -e "URL:          ${BLUE}$APP_URL${NC}"
echo -e "Profile:      ${YELLOW}$DATABRICKS_PROFILE${NC}"
echo ""
echo "📝 Next Steps:"
echo "1. Access your app at: $APP_URL"
echo "2. Test the endpoints using the web UI"
echo "3. Monitor logs: databricks apps logs $APP_NAME --profile $DATABRICKS_PROFILE"
echo "4. Check status: databricks apps get $APP_NAME --profile $DATABRICKS_PROFILE"
echo ""
echo -e "${GREEN}🚀 Your Zerobus Delta App is ready for use!${NC}"
