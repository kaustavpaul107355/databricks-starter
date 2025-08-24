# Multi-Agent Supervisor Chat App

A Streamlit-based chat interface for your deployed Multi-Agent Supervisor endpoint. This app provides a user-friendly way to interact with your multi-agent system through a web interface.

## 🚀 Features

- **Real-time Chat Interface**: Clean, modern chat UI built with Streamlit
- **Multi-Agent Integration**: Connects directly to your `mas-6c04fa76-endpoint`
- **Chat History**: Persistent conversation history with export functionality
- **Tool Call Visualization**: See when agents use tools (when supported)
- **Responsive Design**: Works on desktop and mobile devices
- **Configuration Validation**: Clear feedback on setup status

## 📋 Prerequisites

- Python 3.11 or above
- Databricks CLI installed and configured
- Access to your Multi-Agent Supervisor endpoint: `mas-6c04fa76-endpoint`
- Personal access token for Databricks workspace

## 🛠️ Local Development Setup

### 1. Clone and Navigate
```bash
cd databricks-apps
```

### 2. Set Up Environment
```bash
# Copy environment template
cp env.example .env

# Edit .env with your actual values
nano .env  # or use your preferred editor
```

Required environment variables:
```bash
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
SERVING_ENDPOINT=mas-6c04fa76-endpoint
```

### 3. Install Dependencies
```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Run Locally
```bash
# Use the convenience script
chmod +x run_local.sh
./run_local.sh

# Or run manually
streamlit run app.py
```

The app will open at `http://localhost:8501`

## 🚀 Deployment to Databricks

### 1. Deploy as Databricks App
```bash
# Make deployment script executable
chmod +x deploy.sh

# Deploy to Databricks
./deploy.sh
```

### 2. Manual Deployment Steps
```bash
# Create the app
databricks apps create --json '{
  "name": "multi-agent-chat-app",
  "resources": [
    {
      "name": "serving-endpoint",
      "serving_endpoint": {
        "name": "mas-6c04fa76-endpoint",
        "permission": "CAN_QUERY"
      }
    }
  ]
}'

# Sync source code
DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
databricks sync . "/Users/$DATABRICKS_USERNAME/multi-agent-chat-app"

# Deploy the app
databricks apps deploy multi-agent-chat-app --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/multi-agent-chat-app"

# Get the app URL
databricks apps get multi-agent-chat-app | jq -r '.url'
```

## 🔧 Configuration

### Environment Variables
- `DATABRICKS_HOST`: Your Databricks workspace URL (without https://)
- `DATABRICKS_TOKEN`: Your personal access token
- `SERVING_ENDPOINT`: Multi-agent supervisor endpoint name (defaults to `mas-6c04fa76-endpoint`)

### App Configuration
The app automatically detects your configuration and provides real-time feedback on the setup status in the sidebar.

## 📱 Usage

1. **Start a Conversation**: Type your message in the chat input at the bottom
2. **View Responses**: See responses from your multi-agent supervisor in real-time
3. **Export Chat**: Use the sidebar to export your conversation history
4. **Clear Chat**: Reset the conversation using the sidebar controls

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│   Streamlit UI  │───▶│  Multi-Agent Chat   │───▶│  Databricks API     │
│                 │    │       App           │    │                     │
└─────────────────┘    └──────────────────────┘    └─────────────────────┘
                                │
                                ▼
                       ┌──────────────────────┐
                       │ Multi-Agent         │
                       │ Supervisor Endpoint │
                       │ mas-6c04fa76-...   │
                       └──────────────────────┘
```

## 🔍 Troubleshooting

### Common Issues

1. **Configuration Errors**
   - Ensure all environment variables are set correctly
   - Verify your Databricks token is valid
   - Check that the endpoint name matches exactly

2. **Connection Issues**
   - Verify network connectivity to your Databricks workspace
   - Check if your token has the necessary permissions
   - Ensure the endpoint is running and accessible

3. **Response Processing Errors**
   - Check the endpoint response format
   - Verify the multi-agent supervisor is functioning correctly

### Debug Mode
Enable debug logging by setting:
```bash
export STREAMLIT_LOG_LEVEL=debug
```

## 📚 API Reference

The app communicates with your multi-agent supervisor endpoint using the standard Databricks serving endpoint API:

- **Endpoint**: `/api/2.0/serving-endpoints/{endpoint_name}/invocations`
- **Method**: POST
- **Headers**: Authorization (Bearer token), Content-Type: application/json
- **Payload**: Messages array in the format expected by your multi-agent system

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## 📄 License

This project is part of your Databricks workspace setup.

## 🆘 Support

For issues related to:
- **Multi-Agent Supervisor**: Check your Databricks workspace logs
- **Chat App**: Review the troubleshooting section above
- **Databricks Platform**: Contact Databricks support

---

**Endpoint**: `mas-6c04fa76-endpoint`  
**Experiment**: `mas-6c04fa76-dev-experiment`  
**Last Updated**: $(date +%Y-%m-%d)
