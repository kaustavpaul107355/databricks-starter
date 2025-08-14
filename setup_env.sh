#!/bin/bash
# Databricks Connect Environment Setup - TEMPLATE
# Copy this file to setup_env_local.sh and fill in your actual values

# Databricks Workspace Configuration
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_ORG_ID="your-org-id"

echo "✅ Databricks Connect environment variables set:"
echo "   Host: $DATABRICKS_HOST"
echo "   Cluster ID: $DATABRICKS_CLUSTER_ID"
echo "   Org ID: $DATABRICKS_ORG_ID"
echo ""
echo "💡 To use these variables in your current session, run:"
echo "   source setup_env_local.sh"
echo ""
echo "💡 To make them permanent, add them to your ~/.bashrc or ~/.zshrc"
echo ""
echo "🔑 To create a long-lived token (365 days), run:"
echo "   databricks tokens create --comment 'Long-lived Token' --lifetime-seconds 31536000 --profile your-profile"
