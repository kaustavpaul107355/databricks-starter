# 🚀 Quick Start Guide

Get your Multi-Agent Supervisor Chat App running in 5 minutes!

## ⚡ Quick Setup

### 1. Prerequisites Check
```bash
# Check Python version (3.9+ required)
python3 --version

# Check if Databricks CLI is installed
databricks --version
```

### 2. One-Command Setup
```bash
# Make setup script executable and run it
chmod +x setup_deployment.sh
./setup_deployment.sh
```

### 3. Configure Environment
```bash
# Edit the .env file with your details
nano .env
```

Fill in your actual values:
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
SERVING_ENDPOINT=mas-6c04fa76-endpoint
WORKSPACE_ID=your-workspace-id
```

### 4. Configure Databricks CLI
```bash
databricks configure
```

### 5. Test Locally
```bash
# Activate virtual environment
source venv/bin/activate

# Test configuration
python test_local.py

# Start the app
streamlit run app.py
```

### 6. Deploy to Databricks
```bash
# Set environment variables
export SERVING_ENDPOINT="mas-6c04fa76-endpoint"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Deploy
./deploy.sh
```

## 🔧 Troubleshooting

### Common Issues

**❌ "MLflow deployment client failed"**
```bash
# Reconfigure Databricks CLI
databricks configure
```

**❌ "Endpoint not accessible"**
- Check if your MAS endpoint exists
- Verify you have CAN_QUERY permission
- Ensure the endpoint is in RUNNING state

**❌ "Environment variables not set"**
```bash
# Check current environment
env | grep DATABRICKS
env | grep SERVING

# Set them manually
export SERVING_ENDPOINT="your-endpoint"
export DATABRICKS_HOST="your-workspace-url"
```

### Getting Help

- 📚 [Full README](README.md)
- 🆘 [Databricks Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app)
- 🐛 Open an issue in the repository

## 🎯 Next Steps

1. **Customize Agents**: Modify the agent selection in `app.py`
2. **Add Features**: Extend the chat interface with new capabilities
3. **Deploy Updates**: Use `./deploy.sh` to push changes
4. **Share**: Grant permissions to team members

---

**Need help?** Check the troubleshooting section or open an issue! 🚀
