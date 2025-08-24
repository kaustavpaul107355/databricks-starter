#!/usr/bin/env python3
"""
Multi-Agent Supervisor Databricks App

A Databricks App that integrates with the Multi-Agent Supervisor Agent Bricks endpoint
to coordinate AI agents for complex tasks.

This app provides:
- Task submission interface for the Multi-Agent Supervisor
- Real-time task monitoring and status updates
- Integration with Databricks Unity Catalog and services
- Web-based UI for interacting with the MAS endpoint
"""

import os
import json
import logging
import streamlit as st
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MultiAgentSupervisorApp:
    """
    Databricks App for interacting with the Multi-Agent Supervisor Agent Bricks endpoint.
    
    This app integrates with Databricks' native Multi-Agent Supervisor service to:
    - Submit complex tasks that require multiple AI agents
    - Monitor task execution and agent coordination
    - View system status and performance metrics
    - Manage agent capabilities and configurations
    """
    
    def __init__(self):
        """Initialize the Multi-Agent Supervisor App."""
        self.workspace_client = self._create_workspace_client()
        self.mas_endpoint_name = os.getenv('MAS_ENDPOINT_NAME', 'mas-6c04fa76-endpoint')
        self.mas_endpoint_url = None
        self._discover_endpoint()
        
    def _create_workspace_client(self) -> WorkspaceClient:
        """Create a Databricks workspace client."""
        try:
            # For Databricks Apps, authentication is handled automatically
            return WorkspaceClient()
        except Exception as e:
            logger.error(f"Failed to create workspace client: {e}")
            st.error(f"Failed to connect to Databricks workspace: {e}")
            return None
    
    def _discover_endpoint(self):
        """Discover the Multi-Agent Supervisor endpoint."""
        if not self.workspace_client:
            return
            
        try:
            # Get endpoint details from Databricks
            endpoint = self.workspace_client.serving_endpoints.get(name=self.mas_endpoint_name)
            
            if endpoint and endpoint.state.value == "READY":
                host = self.workspace_client.config.host
                self.mas_endpoint_url = f"{host}/serving-endpoints/{self.mas_endpoint_name}/invocations"
                logger.info(f"Discovered MAS endpoint: {self.mas_endpoint_url}")
            else:
                logger.warning(f"MAS endpoint {self.mas_endpoint_name} not ready")
                st.warning(f"Multi-Agent Supervisor endpoint '{self.mas_endpoint_name}' is not ready")
                
        except Exception as e:
            logger.error(f"Failed to discover MAS endpoint: {e}")
            st.error(f"Failed to discover Multi-Agent Supervisor endpoint: {e}")
    
    def get_endpoint_status(self) -> Dict[str, Any]:
        """Get the current status of the MAS endpoint."""
        if not self.workspace_client or not self.mas_endpoint_url:
            return {"status": "not_configured"}
            
        try:
            endpoint = self.workspace_client.serving_endpoints.get(name=self.mas_endpoint_name)
            
            return {
                "endpoint_name": self.mas_endpoint_name,
                "state": endpoint.state.value if endpoint.state else "unknown",
                "url": self.mas_endpoint_url,
                "ready": endpoint.state.value == "READY" if endpoint.state else False
            }
            
        except Exception as e:
            logger.error(f"Failed to get endpoint status: {e}")
            return {"status": "error", "error": str(e)}
    
    def submit_task(self, task_description: str, priority: str = "medium", 
                   parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Submit a task to the Multi-Agent Supervisor endpoint.
        
        Args:
            task_description: Description of the task to be performed
            priority: Task priority (low, medium, high, critical)
            parameters: Additional task parameters
            
        Returns:
            Response from the MAS endpoint
        """
        if not self.mas_endpoint_url:
            return {"success": False, "error": "Endpoint not configured"}
            
        try:
            # Prepare request payload for the MAS endpoint
            payload = {
                "dataframe_records": [{
                    "task_description": task_description,
                    "priority": priority,
                    "parameters": parameters or {},
                    "timestamp": datetime.now().isoformat()
                }]
            }
            
            # Get authentication token
            token = self.workspace_client.config.token
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Send request to MAS endpoint
            response = requests.post(
                self.mas_endpoint_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully submitted task to MAS endpoint")
            return {
                "success": True,
                "result": result,
                "task_id": result.get("task_id", "unknown"),
                "timestamp": datetime.now().isoformat()
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error submitting task: {e}")
            return {
                "success": False,
                "error": f"HTTP error: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error submitting task: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get the status of a submitted task."""
        if not self.mas_endpoint_url:
            return {"success": False, "error": "Endpoint not configured"}
            
        try:
            # Query task status from MAS endpoint
            payload = {
                "dataframe_records": [{
                    "action": "get_task_status",
                    "task_id": task_id,
                    "timestamp": datetime.now().isoformat()
                }]
            }
            
            token = self.workspace_client.config.token
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(
                self.mas_endpoint_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return {
                "success": True,
                "status": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics from the MAS endpoint."""
        if not self.mas_endpoint_url:
            return {"success": False, "error": "Endpoint not configured"}
            
        try:
            payload = {
                "dataframe_records": [{
                    "action": "get_system_metrics",
                    "timestamp": datetime.now().isoformat()
                }]
            }
            
            token = self.workspace_client.config.token
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(
                self.mas_endpoint_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return {
                "success": True,
                "metrics": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting system metrics: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Multi-Agent Supervisor",
        page_icon="🤖",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🤖 Multi-Agent Supervisor")
    st.markdown("Coordinate AI agents for complex tasks using Databricks Agent Bricks")
    
    # Initialize the app
    app = MultiAgentSupervisorApp()
    
    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page",
        ["Dashboard", "Submit Task", "Task Monitor", "System Status"]
    )
    
    if page == "Dashboard":
        show_dashboard(app)
    elif page == "Submit Task":
        show_task_submission(app)
    elif page == "Task Monitor":
        show_task_monitor(app)
    elif page == "System Status":
        show_system_status(app)

def show_dashboard(app: MultiAgentSupervisorApp):
    """Show the main dashboard."""
    st.header("📊 Dashboard")
    
    # Endpoint status
    status = app.get_endpoint_status()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if status.get("ready", False):
            st.success("✅ Endpoint Ready")
        else:
            st.error("❌ Endpoint Not Ready")
    
    with col2:
        st.metric("Endpoint", status.get("endpoint_name", "Unknown"))
    
    with col3:
        st.metric("State", status.get("state", "Unknown"))
    
    # Quick task submission
    st.subheader("🚀 Quick Task Submission")
    
    with st.form("quick_task"):
        task_desc = st.text_area(
            "Task Description",
            placeholder="Describe the task you want the AI agents to perform...",
            height=100
        )
        
        priority = st.selectbox("Priority", ["low", "medium", "high", "critical"])
        
        submitted = st.form_submit_button("Submit Task")
        
        if submitted and task_desc:
            with st.spinner("Submitting task to Multi-Agent Supervisor..."):
                result = app.submit_task(task_desc, priority)
                
                if result["success"]:
                    st.success(f"Task submitted successfully! Task ID: {result['task_id']}")
                else:
                    st.error(f"Failed to submit task: {result['error']}")

def show_task_submission(app: MultiAgentSupervisorApp):
    """Show the task submission page."""
    st.header("📝 Submit New Task")
    
    with st.form("task_submission"):
        st.subheader("Task Information")
        
        task_name = st.text_input("Task Name", placeholder="e.g., Market Analysis Report")
        task_description = st.text_area(
            "Task Description",
            placeholder="Provide a detailed description of what you want the AI agents to accomplish...",
            height=150
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            priority = st.selectbox("Priority", ["low", "medium", "high", "critical"])
            category = st.selectbox("Category", [
                "data_analysis", "market_research", "document_processing", 
                "ml_training", "quality_assurance", "automation", "other"
            ])
        
        with col2:
            expected_duration = st.selectbox("Expected Duration", [
                "minutes", "hours", "days", "unknown"
            ])
            complexity = st.selectbox("Complexity", ["simple", "moderate", "complex", "very_complex"])
        
        # Advanced parameters
        st.subheader("Advanced Parameters")
        
        with st.expander("Additional Parameters"):
            custom_params = st.text_area(
                "Custom Parameters (JSON)",
                placeholder='{"key": "value"}',
                height=100
            )
            
            try:
                if custom_params:
                    json.loads(custom_params)
                    st.success("✅ Valid JSON")
                else:
                    custom_params = "{}"
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON format")
                custom_params = "{}"
        
        submitted = st.form_submit_button("Submit Task to Multi-Agent Supervisor")
        
        if submitted and task_description:
            with st.spinner("Submitting task..."):
                # Prepare parameters
                parameters = {
                    "task_name": task_name,
                    "category": category,
                    "expected_duration": expected_duration,
                    "complexity": complexity,
                    "custom_params": custom_params
                }
                
                result = app.submit_task(task_description, priority, parameters)
                
                if result["success"]:
                    st.success("🎉 Task submitted successfully!")
                    st.info(f"**Task ID:** {result['task_id']}")
                    st.info(f"**Submitted at:** {result['timestamp']}")
                    
                    # Show next steps
                    st.subheader("Next Steps")
                    st.markdown("""
                    1. **Monitor Progress**: Go to the Task Monitor page to track your task
                    2. **Check Status**: The Multi-Agent Supervisor will coordinate AI agents
                    3. **Review Results**: Results will be available when the task completes
                    """)
                else:
                    st.error(f"❌ Failed to submit task: {result['error']}")

def show_task_monitor(app: MultiAgentSupervisorApp):
    """Show the task monitoring page."""
    st.header("📊 Task Monitor")
    
    # Task ID input for monitoring
    task_id = st.text_input("Enter Task ID to monitor", placeholder="e.g., task-abc123")
    
    if task_id:
        if st.button("Check Task Status"):
            with st.spinner("Checking task status..."):
                status = app.get_task_status(task_id)
                
                if status["success"]:
                    st.success("✅ Task status retrieved")
                    
                    # Display task information
                    task_info = status["status"]
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Task ID", task_id)
                        st.metric("Status", task_info.get("status", "Unknown"))
                    
                    with col2:
                        st.metric("Progress", f"{task_info.get('progress', 0)}%")
                        st.metric("Last Updated", task_info.get("last_updated", "Unknown"))
                    
                    # Show detailed information
                    with st.expander("Task Details"):
                        st.json(task_info)
                else:
                    st.error(f"❌ Failed to get task status: {status['error']}")
    
    # Recent tasks (placeholder for future enhancement)
    st.subheader("📋 Recent Tasks")
    st.info("Recent task history will be displayed here in future versions.")

def show_system_status(app: MultiAgentSupervisorApp):
    """Show the system status page."""
    st.header("🔍 System Status")
    
    # Endpoint status
    st.subheader("Multi-Agent Supervisor Endpoint")
    status = app.get_endpoint_status()
    
    if status.get("ready", False):
        st.success("✅ Endpoint is ready and accepting requests")
    else:
        st.warning("⚠️ Endpoint is not ready")
    
    # Status details
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Endpoint Name", status.get("endpoint_name", "Unknown"))
        st.metric("State", status.get("state", "Unknown"))
    
    with col2:
        st.metric("URL", status.get("url", "Not configured")[:50] + "..." if status.get("url") else "Not configured")
        st.metric("Status", "Ready" if status.get("ready", False) else "Not Ready")
    
    # System metrics
    st.subheader("📈 System Metrics")
    
    if st.button("Refresh Metrics"):
        with st.spinner("Fetching system metrics..."):
            metrics = app.get_system_metrics()
            
            if metrics["success"]:
                st.success("✅ Metrics retrieved successfully")
                
                # Display metrics
                metrics_data = metrics["metrics"]
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Active Agents", metrics_data.get("active_agents", 0))
                    st.metric("Pending Tasks", metrics_data.get("pending_tasks", 0))
                
                with col2:
                    st.metric("Completed Tasks", metrics_data.get("completed_tasks", 0))
                    st.metric("Failed Tasks", metrics_data.get("failed_tasks", 0))
                
                with col3:
                    st.metric("Success Rate", f"{metrics_data.get('success_rate', 0)}%")
                    st.metric("Avg Response Time", f"{metrics_data.get('avg_response_time', 0)}s")
                
                # Detailed metrics
                with st.expander("Detailed Metrics"):
                    st.json(metrics_data)
            else:
                st.error(f"❌ Failed to get metrics: {metrics['error']}")
    
    # Health check
    st.subheader("🏥 Health Check")
    
    if st.button("Run Health Check"):
        with st.spinner("Running health check..."):
            # Simple health check
            endpoint_status = app.get_endpoint_status()
            
            if endpoint_status.get("ready", False):
                st.success("✅ System is healthy")
                st.info("The Multi-Agent Supervisor endpoint is responding and ready to accept tasks.")
            else:
                st.error("❌ System health check failed")
                st.warning("The Multi-Agent Supervisor endpoint is not ready or not responding.")

if __name__ == "__main__":
    main()
