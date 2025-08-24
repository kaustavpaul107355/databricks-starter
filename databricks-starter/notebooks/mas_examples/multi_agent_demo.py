#!/usr/bin/env python3
"""
Multi-Agent Supervisor Demo Application

This demo shows how to use the Multi-Agent Supervisor system with Databricks.
It demonstrates agent registration, task submission, and system monitoring.
"""

import os
import sys
import time
import uuid
from datetime import datetime
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from src.multi_agent_supervisor import (
    MultiAgentSupervisor, Agent, Task, TaskPriority, AgentStatus,
    create_data_processing_task, create_ml_training_task, create_data_quality_check_task
)

def check_environment():
    """Check if required environment variables are set."""
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables:")
        print("export DATABRICKS_HOST='your-workspace-url'")
        print("export DATABRICKS_CLUSTER_ID='your-cluster-id'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return False
    
    return True

def create_sample_agents():
    """Create sample agents with different capabilities."""
    agents = [
        Agent(
            id="agent-001",
            name="Data Processing Agent",
            capabilities=["data_processing", "spark", "python", "sql"],
            metadata={"specialization": "ETL", "experience_level": "senior"}
        ),
        Agent(
            id="agent-002",
            name="ML Training Agent",
            capabilities=["ml_training", "python", "scikit-learn", "pandas", "numpy"],
            metadata={"specialization": "Machine Learning", "experience_level": "expert"}
        ),
        Agent(
            id="agent-003",
            name="Data Quality Agent",
            capabilities=["data_quality", "validation", "python", "pandas"],
            metadata={"specialization": "Data Validation", "experience_level": "intermediate"}
        ),
        Agent(
            id="agent-004",
            name="General Purpose Agent",
            capabilities=["python", "basic_processing", "file_operations"],
            metadata={"specialization": "General", "experience_level": "junior"}
        )
    ]
    return agents

def create_sample_tasks():
    """Create sample tasks with different priorities and requirements."""
    tasks = [
        create_data_processing_task(
            task_id=f"task-{uuid.uuid4().hex[:8]}",
            data_source="/mnt/data/raw/sales_data",
            processing_logic="aggregate_by_region_and_date",
            output_destination="/mnt/data/processed/sales_aggregated",
            priority=TaskPriority.HIGH
        ),
        create_ml_training_task(
            task_id=f"task-{uuid.uuid4().hex[:8]}",
            model_name="Sales Prediction Model",
            training_data="/mnt/data/processed/sales_aggregated",
            hyperparameters={"n_estimators": 100, "max_depth": 10},
            priority=TaskPriority.CRITICAL
        ),
        create_data_quality_check_task(
            task_id=f"task-{uuid.uuid4().hex[:8]}",
            dataset_path="/mnt/data/raw/customer_data",
            quality_checks=["null_check", "duplicate_check", "format_validation"],
            priority=TaskPriority.MEDIUM
        ),
        create_data_processing_task(
            task_id=f"task-{uuid.uuid4().hex[:8]}",
            data_source="/mnt/data/raw/log_data",
            processing_logic="parse_and_filter_logs",
            output_destination="/mnt/data/processed/logs_parsed",
            priority=TaskPriority.LOW
        )
    ]
    return tasks

def simulate_agent_workflow(supervisor, agent, task):
    """Simulate an agent working on a task."""
    print(f"🤖 Agent {agent.name} starting task: {task.name}")
    
    # Simulate work time based on task complexity
    work_time = len(task.required_capabilities) * 2  # 2 seconds per capability
    time.sleep(work_time)
    
    # Simulate success or failure (90% success rate)
    import random
    if random.random() < 0.9:
        result = {
            "status": "success",
            "processing_time_seconds": work_time,
            "output_size": random.randint(100, 1000),
            "quality_score": random.uniform(0.8, 1.0)
        }
        supervisor.complete_task(task.id, result, agent.id)
        print(f"✅ Agent {agent.name} completed task: {task.name}")
        return True
    else:
        error = "Simulated processing error"
        supervisor.fail_task(task.id, error, agent.id)
        print(f"❌ Agent {agent.name} failed task: {task.name}: {error}")
        return False

def run_interactive_demo():
    """Run an interactive demo of the Multi-Agent Supervisor."""
    print("🚀 Multi-Agent Supervisor Demo")
    print("=" * 50)
    
    # Check environment
    if not check_environment():
        return
    
    try:
        # Initialize Databricks connection
        print("🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Create workspace client
        workspace_client = WorkspaceClient(
            host=os.environ['DATABRICKS_HOST'],
            token=os.environ['DATABRICKS_TOKEN']
        )
        print("✅ Workspace client created")
        
        # Initialize supervisor with MAS endpoint
        print("🔄 Initializing Multi-Agent Supervisor with MAS endpoint...")
        supervisor = MultiAgentSupervisor(workspace_client, spark, "mas-6c04fa76-endpoint")
        print("✅ Supervisor initialized")
        
        # Check MAS endpoint status
        print("🔄 Checking MAS endpoint status...")
        mas_status = supervisor.get_mas_endpoint_status()
        if mas_status.get("available", False):
            print("✅ MAS endpoint is available and healthy")
            print(f"   Endpoint state: {mas_status['endpoint_status'].get('state', 'unknown')}")
        else:
            print(f"⚠️ MAS endpoint not available: {mas_status.get('error', 'Unknown error')}")
            print("   Continuing with local agents only...")
        
        # Query MAS endpoint capabilities
        if mas_status.get("available", False):
            print("\n🔍 Querying MAS endpoint capabilities...")
            mas_capabilities = supervisor.query_mas_capabilities()
            if mas_capabilities.get("success", False):
                print("✅ MAS endpoint capabilities:")
                capabilities = mas_capabilities.get("capabilities", [])
                if capabilities:
                    for cap in capabilities[:5]:  # Show first 5 capabilities
                        print(f"   - {cap}")
                    if len(capabilities) > 5:
                        print(f"   ... and {len(capabilities) - 5} more capabilities")
                else:
                    print("   - No specific capabilities reported")
            else:
                print(f"   ⚠️ Could not query capabilities: {mas_capabilities.get('error', 'Unknown error')}")
        
        # Register agents
        print("\n🤖 Registering local agents...")
        agents = create_sample_agents()
        for agent in agents:
            if supervisor.register_agent(agent):
                print(f"   ✅ {agent.name} registered")
            else:
                print(f"   ❌ Failed to register {agent.name}")
        
        # Submit tasks with hybrid processing
        print("\n📋 Submitting tasks with hybrid processing...")
        tasks = create_sample_tasks()
        for i, task in enumerate(tasks):
            try:
                task_id = supervisor.submit_task(task)
                print(f"   ✅ Task submitted: {task.name} (ID: {task_id})")
                
                # Demonstrate hybrid processing for some tasks
                if i < 2 and mas_status.get("available", False):
                    print(f"   🔄 Testing hybrid processing for {task.name}...")
                    processing_strategy = supervisor.hybrid_task_processing(task)
                    print(f"   📊 Processing strategy: {processing_strategy}")
                    
                    # If submitted to MAS endpoint, show the response
                    if task.metadata.get("submitted_to_mas", False):
                        mas_response = task.metadata.get("mas_endpoint_response", {})
                        if mas_response.get("success", False):
                            print(f"   ✅ Successfully submitted to MAS endpoint")
                        else:
                            print(f"   ⚠️ MAS submission failed: {mas_response.get('error', 'Unknown error')}")
                            
            except Exception as e:
                print(f"   ❌ Failed to submit task {task.name}: {e}")
        
        # Show initial status
        print("\n📊 Initial System Status:")
        status = supervisor.get_system_status()
        for key, value in status.items():
            if key != "timestamp":
                print(f"   {key}: {value}")
        
        # Simulate task execution
        print("\n🔄 Simulating task execution...")
        for _ in range(3):  # Simulate 3 rounds of work
            print(f"\n--- Round {_ + 1} ---")
            
            # Get current status
            status = supervisor.get_system_status()
            print(f"Pending tasks: {status['pending_tasks']}")
            print(f"Available agents: {status['available_agents']}")
            
            # Process available tasks
            if status['pending_tasks'] > 0 and status['available_agents'] > 0:
                # Get next pending task
                pending_tasks = [t for t in supervisor.task_queue if t.status == "pending"]
                if pending_tasks:
                    task = pending_tasks[0]
                    # Find available agent
                    available_agents = [a for a in supervisor.agents.values() if a.status == AgentStatus.IDLE]
                    if available_agents:
                        agent = available_agents[0]
                        # Simulate work
                        simulate_agent_workflow(supervisor, agent, task)
            
            time.sleep(2)  # Wait between rounds
        
        # Show final status
        print("\n📊 Final System Status:")
        final_status = supervisor.get_system_status()
        for key, value in final_status.items():
            if key != "timestamp":
                print(f"   {key}: {value}")
        
        # Show task summary
        print(f"\n📋 Task Summary:")
        print(f"   Completed: {len(supervisor.completed_tasks)}")
        print(f"   Failed: {len(supervisor.failed_tasks)}")
        print(f"   Pending: {len(supervisor.task_queue)}")
        
        if supervisor.completed_tasks:
            print(f"\n✅ Completed Tasks:")
            for task in supervisor.completed_tasks:
                print(f"   - {task.name} (by {task.assigned_agent})")
        
        if supervisor.failed_tasks:
            print(f"\n❌ Failed Tasks:")
            for task in supervisor.failed_tasks:
                print(f"   - {task.name}: {task.error}")
        
        print("\n🎉 Demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

def run_batch_demo():
    """Run a batch demo with predefined scenarios."""
    print("🚀 Multi-Agent Supervisor Batch Demo")
    print("=" * 50)
    
    if not check_environment():
        return
    
    try:
        # Initialize connections
        spark = DatabricksSession.builder.remote().getOrCreate()
        workspace_client = WorkspaceClient(
            host=os.environ['DATABRICKS_HOST'],
            token=os.environ['DATABRICKS_TOKEN']
        )
        
        # Create supervisor
        supervisor = MultiAgentSupervisor(workspace_client, spark)
        
        # Scenario 1: Register agents
        print("\n📋 Scenario 1: Agent Registration")
        agents = create_sample_agents()
        for agent in agents:
            supervisor.register_agent(agent)
        
        # Scenario 2: Submit high-priority tasks
        print("\n📋 Scenario 2: High-Priority Task Submission")
        high_priority_tasks = [
            create_ml_training_task(
                task_id=f"ml-task-{uuid.uuid4().hex[:8]}",
                model_name="Customer Churn Model",
                training_data="/mnt/data/customers",
                hyperparameters={"n_estimators": 200, "max_depth": 15},
                priority=TaskPriority.CRITICAL
            ),
            create_data_processing_task(
                task_id=f"etl-task-{uuid.uuid4().hex[:8]}",
                data_source="/mnt/data/raw/transactions",
                processing_logic="daily_aggregation",
                output_destination="/mnt/data/processed/daily_transactions",
                priority=TaskPriority.HIGH
            )
        ]
        
        for task in high_priority_tasks:
            supervisor.submit_task(task)
        
        # Scenario 3: Monitor system
        print("\n📋 Scenario 3: System Monitoring")
        for i in range(5):
            status = supervisor.get_system_status()
            print(f"   Round {i+1}: {status['pending_tasks']} pending, {status['available_agents']} available")
            time.sleep(1)
        
        print("\n✅ Batch demo completed!")
        
    except Exception as e:
        print(f"❌ Batch demo failed: {e}")

def run_mas_endpoint_demo():
    """Run a demo specifically showcasing MAS endpoint integration."""
    print("🚀 Multi-Agent Supervisor + MAS Endpoint Demo")
    print("=" * 60)
    
    if not check_environment():
        return
    
    try:
        # Initialize connections
        print("🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        workspace_client = WorkspaceClient(
            host=os.environ['DATABRICKS_HOST'],
            token=os.environ['DATABRICKS_TOKEN']
        )
        
        # Initialize supervisor with MAS endpoint
        print("🔄 Initializing Multi-Agent Supervisor with MAS endpoint...")
        supervisor = MultiAgentSupervisor(workspace_client, spark, "mas-6c04fa76-endpoint")
        
        # Test MAS endpoint connection
        print("\n🧪 Testing MAS Endpoint Connection...")
        mas_status = supervisor.get_mas_endpoint_status()
        
        if mas_status.get("available", False):
            print("✅ MAS endpoint is available!")
            print(f"   State: {mas_status['endpoint_status'].get('state', 'unknown')}")
            print(f"   URL: {mas_status['endpoint_status'].get('url', 'unknown')}")
            
            # Query capabilities
            print("\n🔍 Querying MAS endpoint capabilities...")
            capabilities = supervisor.query_mas_capabilities()
            if capabilities.get("success", False):
                print("✅ Available capabilities:")
                for cap in capabilities.get("capabilities", [])[:10]:
                    print(f"   - {cap}")
            
            # Test task submission
            print("\n📋 Testing task submission to MAS endpoint...")
            test_task = create_data_processing_task(
                task_id="mas-test-001",
                data_source="/test/sample_data",
                processing_logic="data_quality_check",
                output_destination="/test/output",
                priority=TaskPriority.HIGH
            )
            
            mas_response = supervisor.submit_task_to_mas_endpoint(test_task)
            if mas_response.get("success", False):
                print("✅ Successfully submitted test task to MAS endpoint")
                print(f"   Response: {mas_response.get('result', {})}")
            else:
                print(f"❌ Failed to submit task: {mas_response.get('error', 'Unknown error')}")
                
        else:
            print(f"❌ MAS endpoint not available: {mas_status.get('error', 'Unknown error')}")
            print("   Please check your endpoint configuration and status")
            
        print("\n🎉 MAS endpoint demo completed!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to run the demo."""
    print("🚀 Multi-Agent Supervisor Demo Application")
    print("=" * 60)
    print("Choose demo mode:")
    print("1. Interactive Demo (recommended)")
    print("2. Batch Demo")
    print("3. MAS Endpoint Demo (NEW!)")
    print("4. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == "1":
                run_interactive_demo()
                break
            elif choice == "2":
                run_batch_demo()
                break
            elif choice == "3":
                run_mas_endpoint_demo()
                break
            elif choice == "4":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            break

if __name__ == "__main__":
    main()
