# Multi-Agent Supervisor Chat App

A modern Streamlit-based chat application that provides an intuitive interface for querying the Multi-Agent Supervisor (MAS) endpoint using Databricks' MLflow deployment client capabilities.

## 🚀 Features

* **Modern UI**: Beautiful, responsive chat interface with custom styling (following GitHub project pattern)
* **MAS Integration**: Powered by Databricks MLflow model serving endpoints
* **Real Agent Coordination**: Query and retrieve information from your actual MAS agents
* **User Authentication**: Built-in user identification and session management
* **Real-time Chat**: Interactive conversation flow with message history
* **Deployment Ready**: Configured for cloud deployment with environment variables

## 📋 Prerequisites

* Python 3.9+
* Databricks workspace with model serving capabilities
* Access to MAS endpoint: `mas-6c04fa76-endpoint`
* MLflow deployment client configured

## 🛠️ Installation

1. **Clone the repository**  
```bash
git clone <repository-url>  
cd databricks-app-mas
```

2. **Install dependencies**  
```bash
pip install -r requirements.txt
```

3. **Set up environment variables**  
```bash
export SERVING_ENDPOINT="mas-6c04fa76-endpoint"
export DATABRICKS_HOST="https://e2-demo-field-eng.cloud.databricks.com"
export WORKSPACE_ID="1444828305810485"
```

## 🚀 Usage

### Local Development

1. **Start the application**  
```bash
streamlit run app.py
```

2. **Open your browser**  
Navigate to `http://localhost:8501`

### Cloud Deployment

The app is configured for deployment with the following files:

* `app.yaml`: Deployment configuration for cloud platforms
* `requirements.txt`: Python dependencies
* Environment variables configured for MAS endpoint integration

## 🏗️ Architecture

### Core Components

* **`app.py`**: Main Streamlit application with UI and chat logic
* **`requirements.txt`**: Python dependencies (Streamlit + MLflow)
* **`app.yaml`**: Deployment configuration
* **`databricks.yml`**: Databricks Asset Bundle configuration

### Key Features

* **Session Management**: Persistent chat history and user state
* **Agent Configuration**: Dynamic selection of MAS child agents
* **Responsive Design**: Mobile-friendly interface with custom CSS styling
* **Error Handling**: Graceful error handling and user feedback
* **Real MAS Integration**: Uses MLflow deployment client for endpoint calls

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
        "workspace_id": WORKSPACE_ID
    }
)
```

## 🚀 Deployment

### Local Development

```bash
streamlit run app.py
```

### Cloud Deployment

1. Configure your cloud platform (Google Cloud, AWS, Azure)
2. Set environment variables
3. Deploy using the provided `app.yaml` configuration

### Databricks Apps Deployment

```bash
databricks bundle deploy
databricks apps deploy multi-agent-supervisor-app --source-code-path "/path/to/bundle" --mode SNAPSHOT
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

* Check the Databricks documentation for model serving
* Review the Streamlit documentation for UI components
* Open an issue in the repository

---

**Note**: This application requires proper configuration of your MAS endpoint and MLflow deployment client. Ensure all prerequisites are met before deployment.

## 🔗 Related Projects

* [Simple RAG Chatbot](https://github.com/kaustavpaul107355/simple-rag-chatbot) - Working pattern reference
* [Databricks Multi-Agent Supervisor](https://docs.databricks.com/en/machine-learning/llm/multi-agent-supervisor.html) - Official documentation
