#!/usr/bin/env python3
"""
Multi-Agent Supervisor Client Demo

This script demonstrates how to use the Multi-Agent Supervisor client
to interact with the Multi-Agent Supervisor Agent Bricks endpoint.
"""

import os
import sys
from databricks.sdk import WorkspaceClient
from mas_client import MultiAgentSupervisorClient, submit_analysis_task, submit_research_task

def check_environment():
    """Check if required environment variables are set."""
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables:")
        print("export DATABRICKS_HOST='your-workspace-url'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return False
    
    print("✅ Environment variables found")
    return True

def main():
    """Main demonstration function."""
    print("🤖 Multi-Agent Supervisor Client Demo")
    print("=====================================")
    
    # Setup environment
    if not check_environment():
        return
    
    try:
        # Create Databricks workspace client
        print("\n🔄 Connecting to Databricks workspace...")
        workspace_client = WorkspaceClient()
        print("✅ Connected to Databricks workspace")
        
        # Create Multi-Agent Supervisor client
        print("\n🔧 Initializing Multi-Agent Supervisor client...")
        mas_client = MultiAgentSupervisorClient(workspace_client)
        
        # Check endpoint status
        print("\n🔍 Checking Multi-Agent Supervisor endpoint status...")
        endpoint_info = mas_client.get_endpoint_info()
        
        if mas_client.is_ready():
            print("✅ Multi-Agent Supervisor endpoint is ready")
            print(f"   Endpoint: {endpoint_info['endpoint_name']}")
            print(f"   State: {endpoint_info['state']}")
            print(f"   URL: {endpoint_info['url']}")
        else:
            print("❌ Multi-Agent Supervisor endpoint is not ready")
            print(f"   Status: {endpoint_info}")
            print("\nPlease ensure:")
            print("1. Multi-Agent Supervisor is properly configured in your workspace")
            print("2. The endpoint name is correct")
            print("3. You have proper permissions to access the endpoint")
            return
        
        # Perform health check
        print("\n🏥 Performing health check...")
        health = mas_client.health_check()
        print(f"   Overall health: {health['overall_health']}")
        print(f"   Endpoint ready: {health['endpoint_ready']}")
        
        if health['overall_health'] != 'healthy':
            print("⚠️  System health check indicates issues")
            print(f"   Details: {health}")
            return
        
        # Demo 1: Submit a simple task
        print("\n🚀 Demo 1: Submitting a simple task...")
        simple_task = mas_client.submit_task(
            task_description="Please provide a brief overview of current market trends in the technology sector.",
            priority="medium",
            task_name="Market Overview",
            category="market_research"
        )
        
        if simple_task['success']:
            print("✅ Simple task submitted successfully!")
            print(f"   Task ID: {simple_task['task_id']}")
            print(f"   Endpoint: {simple_task['endpoint']}")
            print(f"   Timestamp: {simple_task['timestamp']}")
        else:
            print(f"❌ Failed to submit simple task: {simple_task['error']}")
        
        # Demo 2: Submit an analysis task
        print("\n📊 Demo 2: Submitting an analysis task...")
        analysis_task = submit_analysis_task(
            mas_client,
            analysis_type="comprehensive data quality assessment",
            data_source="customer transaction database",
            requirements=[
                "Check for data completeness and null values",
                "Validate data types and formats",
                "Identify duplicate records",
                "Assess data consistency across tables",
                "Generate quality score and recommendations"
            ],
            priority="high"
        )
        
        if analysis_task['success']:
            print("✅ Analysis task submitted successfully!")
            print(f"   Task ID: {analysis_task['task_id']}")
            print(f"   Category: {analysis_task['result'].get('category', 'unknown')}")
        else:
            print(f"❌ Failed to submit analysis task: {analysis_task['error']}")
        
        # Demo 3: Submit a research task
        print("\n🔬 Demo 3: Submitting a research task...")
        research_task = submit_research_task(
            mas_client,
            research_topic="Impact of AI on Financial Services",
            scope="Focus on banking, insurance, and investment sectors from 2020-2024",
            deliverables=[
                "Executive summary of key findings",
                "Detailed analysis of AI adoption trends",
                "Case studies of successful implementations",
                "Risk assessment and mitigation strategies",
                "Future outlook and recommendations"
            ],
            priority="critical"
        )
        
        if research_task['success']:
            print("✅ Research task submitted successfully!")
            print(f"   Task ID: {research_task['task_id']}")
            print(f"   Priority: {research_task['result'].get('priority', 'unknown')}")
        else:
            print(f"❌ Failed to submit research task: {research_task['error']}")
        
        # Demo 4: Get system status
        print("\n📈 Demo 4: Getting system status...")
        system_status = mas_client.get_system_status()
        
        if system_status['success']:
            print("✅ System status retrieved successfully!")
            print(f"   Status data: {system_status['system_status']}")
        else:
            print(f"❌ Failed to get system status: {system_status['error']}")
        
        # Summary
        print("\n🎉 Demo completed successfully!")
        print("=" * 50)
        print("Tasks submitted:")
        
        tasks = [simple_task, analysis_task, research_task]
        for i, task in enumerate(tasks, 1):
            if task['success']:
                print(f"   {i}. {task['task_id']} - ✅ Success")
            else:
                print(f"   {i}. ❌ Failed - {task['error']}")
        
        print("\n🔧 Next steps:")
        print("1. Monitor task progress using the Task Monitor in the Databricks App")
        print("2. Check task status using the get_task_status() method")
        print("3. View results when tasks complete")
        print("4. Explore the Streamlit app interface for a full user experience")
        
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your Databricks workspace configuration")
        print("2. Check that Multi-Agent Supervisor is properly set up")
        print("3. Ensure you have proper permissions")
        print("4. Review the README for setup instructions")

if __name__ == "__main__":
    main()
