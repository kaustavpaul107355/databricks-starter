"""
Multi-Agent Supervisor Client Library

A simplified client library for interacting with the Multi-Agent Supervisor 
Agent Bricks endpoint in Databricks.

This library provides a clean interface for:
- Submitting tasks to the Multi-Agent Supervisor
- Monitoring task execution and status
- Querying system metrics and health
- Managing agent interactions
"""

import os
import json
import logging
import requests
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MultiAgentSupervisorClient:
    """
    Client for the Multi-Agent Supervisor Agent Bricks endpoint.
    
    This client provides a simplified interface to interact with Databricks'
    native Multi-Agent Supervisor service, which coordinates multiple AI agents
    to complete complex tasks.
    """
    
    def __init__(self, workspace_client: WorkspaceClient, endpoint_name: str = None):
        """
        Initialize the Multi-Agent Supervisor client.
        
        Args:
            workspace_client: Databricks workspace client
            endpoint_name: Name of the MAS endpoint (defaults to environment variable)
        """
        self.workspace_client = workspace_client
        self.endpoint_name = endpoint_name or os.getenv('MAS_ENDPOINT_NAME', 'mas-6c04fa76-endpoint')
        self.endpoint_url = None
        
        # Discover and configure the endpoint
        self._discover_endpoint()
        
        logger.info(f"Multi-Agent Supervisor client initialized for endpoint: {self.endpoint_name}")
    
    def _discover_endpoint(self):
        """Discover and configure the Multi-Agent Supervisor endpoint."""
        try:
            # Get endpoint details from Databricks
            endpoint = self.workspace_client.serving_endpoints.get(name=self.endpoint_name)
            
            if endpoint and endpoint.state.value == "READY":
                host = self.workspace_client.config.host
                self.endpoint_url = f"{host}/serving-endpoints/{self.endpoint_name}/invocations"
                logger.info(f"Discovered MAS endpoint: {self.endpoint_url}")
            else:
                logger.warning(f"MAS endpoint {self.endpoint_name} not ready (state: {endpoint.state.value if endpoint else 'unknown'})")
                self.endpoint_url = None
                
        except Exception as e:
            logger.error(f"Failed to discover MAS endpoint: {e}")
            self.endpoint_url = None
    
    def is_ready(self) -> bool:
        """Check if the Multi-Agent Supervisor endpoint is ready."""
        return self.endpoint_url is not None
    
    def get_endpoint_info(self) -> Dict[str, Any]:
        """Get information about the Multi-Agent Supervisor endpoint."""
        if not self.is_ready():
            return {"status": "not_configured", "error": "Endpoint not discovered"}
        
        try:
            endpoint = self.workspace_client.serving_endpoints.get(name=self.endpoint_name)
            
            return {
                "endpoint_name": self.endpoint_name,
                "state": endpoint.state.value if endpoint.state else "unknown",
                "url": self.endpoint_url,
                "ready": endpoint.state.value == "READY" if endpoint.state else False,
                "creation_timestamp": endpoint.creation_timestamp,
                "last_updated_timestamp": endpoint.last_updated_timestamp
            }
            
        except Exception as e:
            logger.error(f"Failed to get endpoint info: {e}")
            return {"status": "error", "error": str(e)}
    
    def submit_task(self, 
                   task_description: str,
                   priority: str = "medium",
                   task_name: str = None,
                   category: str = None,
                   parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Submit a task to the Multi-Agent Supervisor.
        
        Args:
            task_description: Detailed description of the task to be performed
            priority: Task priority (low, medium, high, critical)
            task_name: Optional name for the task
            category: Optional task category
            parameters: Additional task parameters
            
        Returns:
            Response from the Multi-Agent Supervisor endpoint
        """
        if not self.is_ready():
            return {
                "success": False, 
                "error": "Multi-Agent Supervisor endpoint not ready. Please check configuration."
            }
        
        try:
            # Prepare task payload
            task_data = {
                "task_description": task_description,
                "priority": priority,
                "timestamp": datetime.now().isoformat()
            }
            
            if task_name:
                task_data["task_name"] = task_name
            if category:
                task_data["category"] = category
            if parameters:
                task_data["parameters"] = parameters
            
            # Prepare request payload for the MAS endpoint
            payload = {
                "dataframe_records": [task_data]
            }
            
            # Get authentication token
            token = self.workspace_client.config.token
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Submit task to Multi-Agent Supervisor
            response = requests.post(
                self.endpoint_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully submitted task to Multi-Agent Supervisor")
            
            return {
                "success": True,
                "task_id": result.get("task_id", "unknown"),
                "result": result,
                "timestamp": datetime.now().isoformat(),
                "endpoint": self.endpoint_name
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
        """
        Get the status of a submitted task.
        
        Args:
            task_id: ID of the task to query
            
        Returns:
            Task status information from the Multi-Agent Supervisor
        """
        if not self.is_ready():
            return {
                "success": False, 
                "error": "Multi-Agent Supervisor endpoint not ready"
            }
        
        try:
            # Query task status from Multi-Agent Supervisor
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
                self.endpoint_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return {
                "success": True,
                "task_id": task_id,
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
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get system status and metrics from the Multi-Agent Supervisor.
        
        Returns:
            System status and performance metrics
        """
        if not self.is_ready():
            return {
                "success": False, 
                "error": "Multi-Agent Supervisor endpoint not ready"
            }
        
        try:
            # Query system status from Multi-Agent Supervisor
            payload = {
                "dataframe_records": [{
                    "action": "get_system_status",
                    "timestamp": datetime.now().isoformat()
                }]
            }
            
            token = self.workspace_client.config.token
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(
                self.endpoint_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            return {
                "success": True,
                "system_status": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a comprehensive health check on the Multi-Agent Supervisor.
        
        Returns:
            Health check results including endpoint status and connectivity
        """
        try:
            # Check endpoint discovery
            endpoint_info = self.get_endpoint_info()
            
            # Check system status if endpoint is ready
            system_status = None
            if self.is_ready():
                system_status = self.get_system_status()
            
            # Determine overall health
            overall_health = "healthy"
            if not self.is_ready():
                overall_health = "unhealthy"
            elif endpoint_info.get("state") != "READY":
                overall_health = "degraded"
            
            return {
                "overall_health": overall_health,
                "endpoint_info": endpoint_info,
                "system_status": system_status,
                "endpoint_ready": self.is_ready(),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "overall_health": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

# Utility functions for common operations
def create_mas_client(workspace_client: WorkspaceClient, endpoint_name: str = None) -> MultiAgentSupervisorClient:
    """
    Create a Multi-Agent Supervisor client.
    
    Args:
        workspace_client: Databricks workspace client
        endpoint_name: Optional MAS endpoint name
        
    Returns:
        Configured Multi-Agent Supervisor client
    """
    return MultiAgentSupervisorClient(workspace_client, endpoint_name)

def submit_simple_task(client: MultiAgentSupervisorClient,
                      description: str,
                      priority: str = "medium") -> Dict[str, Any]:
    """
    Submit a simple task to the Multi-Agent Supervisor.
    
    Args:
        client: Multi-Agent Supervisor client
        description: Task description
        priority: Task priority
        
    Returns:
        Task submission result
    """
    return client.submit_task(
        task_description=description,
        priority=priority
    )

def submit_analysis_task(client: MultiAgentSupervisorClient,
                        analysis_type: str,
                        data_source: str,
                        requirements: List[str],
                        priority: str = "medium") -> Dict[str, Any]:
    """
    Submit an analysis task to the Multi-Agent Supervisor.
    
    Args:
        client: Multi-Agent Supervisor client
        analysis_type: Type of analysis to perform
        data_source: Source data description
        requirements: List of analysis requirements
        priority: Task priority
        
    Returns:
        Task submission result
    """
    task_description = f"""
    Perform {analysis_type} analysis on {data_source}.
    
    Requirements:
    {chr(10).join(f"- {req}" for req in requirements)}
    
    Please coordinate with appropriate AI agents to complete this analysis
    and provide comprehensive results.
    """
    
    parameters = {
        "analysis_type": analysis_type,
        "data_source": data_source,
        "requirements": requirements,
        "category": "data_analysis"
    }
    
    return client.submit_task(
        task_description=task_description,
        priority=priority,
        parameters=parameters
    )

def submit_research_task(client: MultiAgentSupervisorClient,
                        research_topic: str,
                        scope: str,
                        deliverables: List[str],
                        priority: str = "medium") -> Dict[str, Any]:
    """
    Submit a research task to the Multi-Agent Supervisor.
    
    Args:
        client: Multi-Agent Supervisor client
        research_topic: Topic to research
        scope: Research scope and boundaries
        deliverables: Expected research deliverables
        priority: Task priority
        
    Returns:
        Task submission result
    """
    task_description = f"""
    Conduct research on: {research_topic}
    
    Scope: {scope}
    
    Expected deliverables:
    {chr(10).join(f"- {deliverable}" for deliverable in deliverables)}
    
    Please coordinate with research and analysis agents to gather information,
    analyze findings, and provide comprehensive research results.
    """
    
    parameters = {
        "research_topic": research_topic,
        "scope": scope,
        "deliverables": deliverables,
        "category": "research"
    }
    
    return client.submit_task(
        task_description=task_description,
        priority=priority,
        parameters=parameters
    )
