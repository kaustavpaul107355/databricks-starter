# Databricks Apps

This directory contains various Databricks applications and utilities.

## 📁 Folder Structure

### `multi-agent-supervisor-chat/`
The main multi-agent supervisor chat application with enhanced UI/UX.

**Contents:**
- `app.py` - Main application file (enhanced version)
- `app_enhanced.py` - Full enhanced version with advanced features
- `app_simple_enhanced.py` - Simplified enhanced version
- `app_minimal.py` - Minimal working version
- `app.yaml` - Databricks App configuration
- `requirements.txt` - Python dependencies
- `messages.py` - Message handling utilities
- `model_serving_utils.py` - Model serving utilities
- `deploy.sh` - Deployment script
- `run_local.sh` - Local development script
- `test_endpoint.py` - Endpoint testing utility
- `debug_endpoint.py` - Debug endpoint utility
- `README.md` - Application documentation
- `QUICKSTART.md` - Quick start guide
- `.env.example` - Environment variables template

## 🚀 Quick Start

1. Navigate to the specific app folder:
   ```bash
   cd multi-agent-supervisor-chat
   ```

2. Set up environment variables:
   ```bash
   cp env.example .env
   # Edit .env with your actual values
   ```

3. Run locally:
   ```bash
   ./run_local.sh
   ```

4. Deploy to Databricks:
   ```bash
   ./deploy.sh
   ```

## 🔧 Development

Each app folder contains its own complete set of files for development and deployment. The parent directory serves as a workspace for managing multiple Databricks applications.
