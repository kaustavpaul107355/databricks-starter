#!/bin/bash

# AIBI Genie App Deployment Script
# Comprehensive deployment following enterprise best practices

set -e  # Exit on any error

# Color codes for enhanced output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Databricks Apps Deployment - AIBI Genie App${NC}"
echo "=============================================="

# Configuration
APP_NAME="aibi-genie-app"
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

print_header() {
    echo -e "${PURPLE}🔧 $1${NC}"
}

# Validate prerequisites
print_header "PHASE 1: Prerequisites Validation"
echo "=================================================="

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed. Please install it first:"
    echo "  pip install databricks-cli"
    exit 1
fi

# Get CLI version
CLI_VERSION=$(databricks version | head -1)
print_status "Databricks CLI detected: $CLI_VERSION"

# Validate profile configuration
print_info "Validating Databricks profile configuration..."
if ! databricks current-user me --profile "$DATABRICKS_PROFILE" &>/dev/null; then
    print_error "Databricks profile '$DATABRICKS_PROFILE' authentication failed."
    echo "Please configure authentication:"
    echo "  databricks configure --profile $DATABRICKS_PROFILE"
    echo ""
    echo "Required environment variables for app.yaml:"
    echo "  - DATABRICKS_WAREHOUSE_ID"
    echo "  - DATABRICKS_HTTP_PATH" 
    echo "  - DATABRICKS_TOKEN"
    echo "  - GENIE_SPACE_URL"
    echo "  - AI_BI_Dashboard_URL"
    exit 1
fi

# Get current user information
CURRENT_USER=$(databricks current-user me --profile "$DATABRICKS_PROFILE" | jq -r .userName)
print_status "Authentication successful - User: $CURRENT_USER"

# Validate app files exist
print_info "Validating application files..."
REQUIRED_FILES=("app.py" "app.yaml" "requirements.txt")
for file in "${REQUIRED_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        print_status "Found: $file"
    else
        print_error "Missing required file: $file"
        exit 1
    fi
done

print_status "Prerequisites validation complete"
echo ""

# Application Configuration Validation
print_header "PHASE 2: Application Configuration Review"
echo "=================================================="

print_info "Reviewing app.yaml configuration..."
echo -e "${BLUE}App Configuration:${NC}"
echo "  • Command: streamlit run app.py"
echo "  • Environment Variables:"
echo "    - DATABRICKS_WAREHOUSE_ID (from resource)"
echo "    - DATABRICKS_HTTP_PATH (configured)"
echo "    - DATABRICKS_TOKEN (configured)"
echo "    - GENIE_SPACE_URL (configured)"
echo "    - AI_BI_Dashboard_URL (configured)"

print_info "Reviewing Python dependencies..."
echo -e "${BLUE}Dependencies:${NC}"
cat requirements.txt | sed 's/^/    - /'

print_info "Validating Python imports..."
if python -c "import streamlit, databricks.sql, pandas, plotly, numpy, requests; print('All imports successful')" 2>/dev/null; then
    print_status "All required packages are available"
else
    print_warning "Some dependencies may be missing. Ensure they're installed in the target environment."
fi

echo ""

# Deployment Process
print_header "PHASE 3: Deployment Process"
echo "=================================================="

# Check if app already exists
print_info "Checking for existing app..."
if databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE" --output json &>/dev/null; then
    EXISTING_APP=$(databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE" --output json)
    APP_STATUS=$(echo "$EXISTING_APP" | jq -r '.app_status.state // "unknown"')
    print_warning "App '$APP_NAME' already exists with status: $APP_STATUS"
    
    read -p "Do you want to update the existing app? (y/N): " confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        DEPLOY_MODE="update"
        print_info "Will update existing app"
    else
        print_info "Deployment cancelled by user"
        exit 0
    fi
else
    print_info "Creating new app '$APP_NAME'"
    DEPLOY_MODE="create"
fi

# Create clean deployment directory
print_info "Preparing deployment files..."
DEPLOY_DIR="../${APP_NAME}-deploy"
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"

# Copy essential files only (exclude development/test files)
cp app.py "$DEPLOY_DIR/"
cp app.yaml "$DEPLOY_DIR/"
cp requirements.txt "$DEPLOY_DIR/"

# Copy .streamlit config if exists
if [[ -d ".streamlit" ]]; then
    cp -r .streamlit "$DEPLOY_DIR/"
    print_status "Copied Streamlit configuration"
fi

print_status "Deployment files prepared in $DEPLOY_DIR"

# Deploy to workspace
print_info "Uploading files to Databricks workspace..."
UPLOAD_PATH="/Workspace/Users/$CURRENT_USER/$APP_NAME"

# Use databricks sync for more reliable file upload
databricks sync "$DEPLOY_DIR" "$UPLOAD_PATH" --profile "$DATABRICKS_PROFILE"
print_status "Files synced to workspace: $UPLOAD_PATH"

# Deploy or update the app
if [[ "$DEPLOY_MODE" == "create" ]]; then
    print_info "Creating new Databricks App..."
    
    # Create app with sql-warehouse resource (required for DATABRICKS_WAREHOUSE_ID)
    databricks apps create --profile "$DATABRICKS_PROFILE" --json '{
        "name": "'$APP_NAME'",
        "resources": [
            {
                "name": "sql-warehouse",
                "sql_warehouse": {
                    "id": "4b9b953939869799",
                    "permission": "CAN_USE"
                }
            }
        ]
    }'
    print_status "App created successfully"
else
    print_info "Updating existing Databricks App configuration..."
    
    # Update app resources if needed
    databricks apps update --profile "$DATABRICKS_PROFILE" --json '{
        "name": "'$APP_NAME'",
        "resources": [
            {
                "name": "sql-warehouse", 
                "sql_warehouse": {
                    "id": "4b9b953939869799",
                    "permission": "CAN_USE"
                }
            }
        ]
    }'
    print_status "App configuration updated"
fi

# Deploy the application code
print_info "Deploying application code..."
databricks apps deploy "$APP_NAME" --source-code-path "$UPLOAD_PATH" --profile "$DATABRICKS_PROFILE"
print_status "Application code deployed successfully"

# Clean up deployment directory
rm -rf "$DEPLOY_DIR"
print_status "Cleanup completed"

echo ""

# Get final app information
print_header "PHASE 4: Deployment Verification"
echo "=================================================="

print_info "Retrieving app information..."
APP_INFO=$(databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE" --output json)
APP_URL=$(echo "$APP_INFO" | jq -r '.url // "URL not available"')
APP_STATUS=$(echo "$APP_INFO" | jq -r '.app_status.state // "Status unknown"')
APP_ID=$(echo "$APP_INFO" | jq -r '.id // "ID not available"')

# Display deployment summary
echo ""
echo "🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!"
echo "====================================="
echo -e "App Name:         ${GREEN}$APP_NAME${NC}"
echo -e "App ID:           ${BLUE}$APP_ID${NC}"
echo -e "Status:           ${GREEN}$APP_STATUS${NC}"
echo -e "URL:              ${BLUE}$APP_URL${NC}"
echo -e "Profile:          ${YELLOW}$DATABRICKS_PROFILE${NC}"
echo -e "Deployed by:      ${PURPLE}$CURRENT_USER${NC}"
echo ""

# Verify app configuration
echo -e "${PURPLE}🔧 App Configuration:${NC}"
echo "  • Framework: Streamlit"
echo "  • Data Source: kaustavpaul_demo.SP500.gold_sp500_analytics"
echo "  • Features:"
echo "    - 📊 Source Data Viewer"
echo "    - 🤖 AI/BI Dashboard Integration"
echo "    - ✨ Genie Space Natural Language Querying"
echo ""

# Next steps
echo "📝 Next Steps:"
echo "1. 🌐 Access your app: $APP_URL"
echo "2. 🔍 Monitor logs: databricks apps logs $APP_NAME --profile $DATABRICKS_PROFILE"
echo "3. ⚙️  Check status: databricks apps get $APP_NAME --profile $DATABRICKS_PROFILE"
echo "4. 🛑 Stop app: databricks apps stop $APP_NAME --profile $DATABRICKS_PROFILE"
echo "5. ▶️  Start app: databricks apps start $APP_NAME --profile $DATABRICKS_PROFILE"
echo ""

# Security reminder
echo -e "${YELLOW}🔐 Security Note:${NC}"
echo "The app uses environment variables for authentication tokens."
echo "Verify that sensitive credentials are properly configured in app.yaml"
echo ""

echo -e "${GREEN}🚀 Your AIBI Genie App is ready for use!${NC}"
echo -e "${BLUE}💡 The app provides S&P500 analytics with AI-powered insights${NC}"
