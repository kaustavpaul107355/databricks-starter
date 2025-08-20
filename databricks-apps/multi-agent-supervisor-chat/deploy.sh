#!/bin/bash

# Multi-Agent Chat App Deployment Script
# This script deploys the chat app to Databricks

set -e

echo "🚀 Deploying Multi-Agent Chat App to Databricks..."

# Check if DATABRICKS_USERNAME is set
if [ -z "$DATABRICKS_USERNAME" ]; then
    echo "Getting current Databricks user..."
    export DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
    echo "Current user: $DATABRICKS_USERNAME"
fi

# Check if SERVING_ENDPOINT is set
if [ -z "$SERVING_ENDPOINT" ]; then
    export SERVING_ENDPOINT="mas-6c04fa76-endpoint"
    echo "Using default endpoint: $SERVING_ENDPOINT"
fi

# Create the app
echo "📱 Creating Databricks App..."
databricks apps create --json "{
  \"name\": \"multi-agent-chat-app\",
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

# Sync the source code
echo "📁 Syncing source code..."
databricks sync . "/Users/$DATABRICKS_USERNAME/multi-agent-chat-app"

# Deploy the app
echo "🚀 Deploying app..."
databricks apps deploy multi-agent-chat-app --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/multi-agent-chat-app"

# Get the app URL
echo "🔗 Getting app URL..."
APP_URL=$(databricks apps get multi-agent-chat-app | jq -r '.url')

echo "✅ Deployment complete!"
echo "🌐 Your app is available at: $APP_URL"
echo ""
echo "📋 Next steps:"
echo "1. Share the app URL with your team"
echo "2. Configure permissions in the Databricks workspace"
echo "3. Test the chat functionality"
