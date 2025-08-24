# Multi-Agent Supervisor Chat App

A modern Streamlit-based chat application that provides an intuitive interface for querying the Multi-Agent Supervisor (MAS) endpoint using Databricks' MLflow deployment client capabilities. This app follows the official [Databricks app template pattern](https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app).

## 🚀 Features

* **Modern UI**: Beautiful, responsive chat interface with custom styling
* **MAS Integration**: Powered by Databricks MLflow model serving endpoints
* **Real Agent Coordination**: Query and retrieve information from your actual MAS agents
* **Streaming Support**: Real-time streaming of agent responses with fallback to non-streaming
* **User Authentication**: Built-in user identification and session management
* **Real-time Chat**: Interactive conversation flow with message history
* **Deployment Ready**: Configured for cloud deployment with environment variables
* **Endpoint Status**: Real-time monitoring of MAS endpoint availability

## 📋 Prerequisites

* Python 3.9+
* Databricks workspace with model serving capabilities
* Access to MAS endpoint: `mas-6c04fa76-endpoint`
* MLflow deployment client configured
* Databricks CLI installed and configured

## 🛠️ Installation

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>  
cd databricks-app-mas

# Run the setup script
chmod +x setup_deployment.sh
./setup_deployment.sh
```

### 2. Configure Environment

Edit the `.env` file with your Databricks configuration:

```bash
# Databricks Workspace Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token

# Multi-Agent Supervisor Endpoint Configuration
SERVING_ENDPOINT=mas-6c04fa76-endpoint
WORKSPACE_ID=your-workspace-id
```

### 3. Configure Databricks CLI

```bash
databricks configure
```

Provide your Databricks host URL and personal access token when prompted.

## 🚀 Usage

### Local Development

1. **Activate virtual environment**  
```bash
source venv/bin/activate
```

2. **Start the application**  
```bash
streamlit run app.py
```

3. **Open your browser**  
Navigate to `http://localhost:8501`

### Cloud Deployment

Deploy the app to Databricks using the deployment script:

```bash
# Set environment variables
export SERVING_ENDPOINT="mas-6c04fa76-endpoint"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Deploy
chmod +x deploy.sh
./deploy.sh
```

## 🏗️ Architecture

### Core Components

* **`app.py`**: Main Streamlit application with UI and chat logic
* **`requirements.txt`**: Python dependencies (Streamlit + MLflow + Requests)
* **`app.yaml`**: Deployment configuration for cloud platforms
* **`databricks.yml`**: Databricks Asset Bundle configuration
* **`deploy.sh`**: Deployment script following Databricks app template pattern
* **`setup_deployment.sh`**: Local development setup script

### Key Features

* **Session Management**: Persistent chat history and user state
* **Agent Configuration**: Dynamic selection of MAS child agents
* **Responsive Design**: Mobile-friendly interface with custom CSS styling
* **Error Handling**: Graceful error handling and user feedback
* **Real MAS Integration**: Uses MLflow deployment client for endpoint calls
* **Streaming Support**: Real-time response streaming with fallback
* **Endpoint Monitoring**: Real-time status checking of MAS endpoint

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| SERVING_ENDPOINT | MAS endpoint name | Yes |
| DATABRICKS_HOST | Databricks workspace URL | Yes |
| WORKSPACE_ID | Databricks workspace ID | Yes |
| STREAMLIT_BROWSER_GATHER_USAGE_STATS | Disable Streamlit usage statistics | No |

### MAS Endpoint

The app requires a Databricks MAS endpoint that supports:

* Multi-agent coordination
* Conversational agent schema
* Message-based input/output format
* Tool call and agent handoff capabilities

## 🎨 UI Features

* **Dark Theme**: Modern dark color scheme with neon accents
* **Chat Interface**: Clean, intuitive chat layout
* **Responsive Design**: Works on desktop and mobile devices
* **Custom Styling**: Tailored CSS for professional appearance
* **Message History**: Persistent conversation threads
* **Agent Status**: Real-time connection status indicators
* **Streaming Toggle**: Enable/disable real-time response streaming

## 🔒 Security

* User authentication via Databricks workspace context
* Environment variable configuration
* Secure MLflow endpoint integration
* Input validation and sanitization

## 📝 API Integration

The app integrates with your MAS endpoint using MLflow's deployment client:

```python
from mlflow.deployments import get_deploy_client

# Query MAS endpoint with messages
client = get_deploy_client('databricks')
response = client.predict(
    endpoint=MAS_ENDPOINT,
    inputs={
        "messages": [{"role": "user", "content": user_message}],
        "selected_agents": selected_agents,
        "max_tokens": 1000,
        "stream": True  # Enable streaming
    }
)
```

## 🚀 Deployment

### Local Development

```bash
source venv/bin/activate
streamlit run app.py
```

### Databricks Apps Deployment

```bash
# Deploy using the provided script
./deploy.sh

# Or manually using Databricks CLI
databricks apps create --json '{
  "name": "multi-agent-supervisor-chat",
  "resources": [
    {
      "name": "serving-endpoint",
      "serving_endpoint": {
        "name": "your-mas-endpoint",
        "permission": "CAN_QUERY"
      }
    }
  ]
}'

# Sync and deploy
databricks sync . "/Users/your-username/multi-agent-supervisor-chat"
databricks apps deploy multi-agent-supervisor-chat --source-code-path "/Workspace/Users/your-username/multi-agent-supervisor-chat"
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👨‍💻 Author

**Kaustav Paul**

## 🆘 Support

For support and questions:

* Check the [Databricks documentation](https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app) for model serving
* Review the [Streamlit documentation](https://docs.streamlit.io/) for UI components
* Open an issue in the repository

## 🔗 Related Projects

* [Databricks App Templates](https://github.com/databricks/app-templates/tree/main/e2e-chatbot-app) - Official app templates
* [Databricks Multi-Agent Supervisor](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor) - Official documentation
* [Databricks Apps](https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app) - App framework documentation

---

**Note**: This application requires proper configuration of your MAS endpoint and MLflow deployment client. Ensure all prerequisites are met before deployment.
