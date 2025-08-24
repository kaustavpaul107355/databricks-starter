# 🚀 Quick Start Guide

Get your Multi-Agent Chat App running in 5 minutes!

## ⚡ Quick Setup

### 1. Configure Environment
```bash
cd databricks-apps
cp env.example .env
```

Edit `.env` with your values:
```bash
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
SERVING_ENDPOINT=mas-6c04fa76-endpoint
```

### 2. Test Connection
```bash
python test_endpoint.py
```

### 3. Run Locally
```bash
./run_local.sh
```

**That's it!** 🎉 Your chat app will open at `http://localhost:8501`

## 🔧 What You Need

- ✅ Python 3.11+
- ✅ Databricks CLI configured
- ✅ Access to `mas-6c04fa76-endpoint`
- ✅ Personal access token

## 🚀 Deploy to Databricks

```bash
./deploy.sh
```

## 📱 Features

- 🤖 Chat with your multi-agent supervisor
- 💬 Persistent conversation history
- 📤 Export chat logs
- 🎨 Clean, modern UI
- 📱 Mobile responsive

## 🆘 Need Help?

- Check the [README.md](README.md) for detailed instructions
- Run `python test_endpoint.py` to diagnose connection issues
- Verify your endpoint is running in Databricks

---

**Endpoint**: `mas-6c04fa76-endpoint`  
**Experiment**: `mas-6c04fa76-dev-experiment`
