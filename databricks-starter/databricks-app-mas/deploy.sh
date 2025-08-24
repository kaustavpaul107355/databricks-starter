#!/bin/bash

# Multi-Agent Supervisor Chat App Deployment Script
# Following the official Databricks app template pattern

set -e

echo "🚀 Deploying Multi-Agent Supervisor Chat App..."

# Check if required environment variables are set
if [ -z "$SERVING_ENDPOINT" ]; then
    echo "❌ Error: SERVING_ENDPOINT environment variable is not set"
    echo "Please set it to your MAS endpoint name (e.g., mas-6c04fa76-endpoint)"
    exit 1
fi

if [ -z "$DATABRICKS_HOST" ]; then
    echo "❌ Error: DATABRICKS_HOST environment variable is not set"
    echo "Please set it to your Databricks workspace URL"
    exit 1
fi

# Set the profile to use
export DATABRICKS_CONFIG_PROFILE="DEFAULT"

# Get current user
echo "👤 Getting current Databricks user..."
DATABRICKS_USERNAME=$(databricks current-user me --profile DEFAULT | jq -r .userName)
echo "✅ Current user: $DATABRICKS_USERNAME"

# Create the Databricks App
echo "📱 Creating Databricks App..."
APP_NAME="multi-agent-supervisor-chat"

# Check if app already exists
if databricks apps get "$APP_NAME" --profile DEFAULT >/dev/null 2>&1; then
    echo "ℹ️  App '$APP_NAME' already exists, updating..."
    databricks apps update "$APP_NAME" --profile DEFAULT --json "{
        \"name\": \"$APP_NAME\",
        \"resources\": [
            {
                \"name\": \"serving-endpoint\",
                \"serving_endpoint\": {
                    \"name\": \"$SERVING_ENDPOINT\",
                    \"permission\": \"CAN_QUERY\"
                }
            }
        ]
    }"
else
    echo "🆕 Creating new app '$APP_NAME'..."
    databricks apps create --profile DEFAULT --json "{
        \"name\": \"$APP_NAME\",
        \"resources\": [
            {
                \"name\": \"serving-endpoint\",
                \"serving_endpoint\": {
                    \"name\": \"$SERVING_ENDPOINT\",
                    \"permission\": \"CAN_QUERY\"
                }
            }
        ]
    }"
fi

# Sync source code to Databricks
echo "📁 Syncing source code to Databricks..."
WORKSPACE_PATH="/Workspace/Users/$DATABRICKS_USERNAME/multi-agent-supervisor-chat"
databricks sync . "$WORKSPACE_PATH" --profile DEFAULT

# Deploy the app
echo "🚀 Deploying app from source code..."
databricks apps deploy "$APP_NAME" --profile DEFAULT --source-code-path "$WORKSPACE_PATH"

# Get the app URL
echo "🔗 Getting app URL..."
APP_URL=$(databricks apps get "$APP_NAME" --profile DEFAULT | jq -r '.url')

echo "✅ Deployment completed successfully!"
echo ""
echo "🎉 Your Multi-Agent Supervisor Chat App is now deployed!"
echo "🌐 App URL: $APP_URL"
echo ""
echo "📋 Next steps:"
echo "1. Open the app URL in your browser"
echo "2. Test the chat functionality with your MAS endpoint"
echo "3. Share the app URL with your team"
echo ""
echo "🔧 To update the app in the future, run this script again"
echo "📚 For more information, see: https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app"
