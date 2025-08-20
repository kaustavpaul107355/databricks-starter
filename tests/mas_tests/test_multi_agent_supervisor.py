#!/usr/bin/env python3
"""
Test script for Multi-Agent Supervisor

This script tests the basic functionality of the Multi-Agent Supervisor system
without requiring a full Databricks connection.
"""

import sys
import os
import unittest
from unittest.mock import Mock, MagicMock
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from multi_agent_supervisor import (
    MultiAgentSupervisor, Agent, Task, TaskPriority, AgentStatus,
    create_data_processing_task, create_ml_training_task, create_data_quality_check_task
)

class TestMultiAgentSupervisor(unittest.TestCase):
    """Test cases for Multi-Agent Supervisor."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock objects
        self.mock_workspace_client = Mock()
        self.mock_spark_session = Mock()
        
        # Create supervisor instance
        self.supervisor = MultiAgentSupervisor(self.mock_workspace_client, self.mock_spark_session)
        
        # Create test agents
        self.data_agent = Agent(
            id="test-agent-001",
            name="Test Data Agent",
            capabilities=["data_processing", "spark", "python"],
            metadata={"test": True}
        )
        
        self.ml_agent = Agent(
            id="test-agent-002",
            name="Test ML Agent",
            capabilities=["ml_training", "python", "scikit-learn"],
            metadata={"test": True}
        )
    
    def test_agent_registration(self):
        """Test agent registration functionality."""
        # Test successful registration
        result = self.supervisor.register_agent(self.data_agent)
        self.assertTrue(result)
        self.assertIn(self.data_agent.id, self.supervisor.agents)
        self.assertEqual(len(self.supervisor.agents), 1)
        
        # Test duplicate registration (should update existing)
        result = self.supervisor.register_agent(self.data_agent)
        self.assertTrue(result)
        self.assertEqual(len(self.supervisor.agents), 1)
    
    def test_agent_unregistration(self):
        """Test agent unregistration functionality."""
        # Register agent first
        self.supervisor.register_agent(self.data_agent)
        self.assertEqual(len(self.supervisor.agents), 1)
        
        # Test successful unregistration
        result = self.supervisor.unregister_agent(self.data_agent.id)
        self.assertTrue(result)
        self.assertEqual(len(self.supervisor.agents), 0)
        
        # Test unregistering non-existent agent
        result = self.supervisor.unregister_agent("non-existent")
        self.assertFalse(result)
    
    def test_task_submission(self):
        """Test task submission functionality."""
        # Create a test task
        task = create_data_processing_task(
            task_id="test-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        
        # Test task submission
        task_id = self.supervisor.submit_task(task)
        self.assertEqual(task_id, task.id)
        self.assertIn(task, self.supervisor.task_queue)
        self.assertEqual(len(self.supervisor.task_queue), 1)
    
    def test_task_validation(self):
        """Test task validation."""
        # Test invalid task (missing ID)
        invalid_task = Task(
            id="",
            name="Invalid Task",
            description="Test invalid task"
        )
        
        with self.assertRaises(ValueError):
            self.supervisor.submit_task(invalid_task)
    
    def test_agent_capability_matching(self):
        """Test agent capability matching."""
        # Register agents
        self.supervisor.register_agent(self.data_agent)
        self.supervisor.register_agent(self.ml_agent)
        
        # Create task requiring data processing
        data_task = create_data_processing_task(
            task_id="data-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        
        # Test capability matching
        can_handle = self.supervisor._agent_can_handle_task(self.data_agent, data_task)
        self.assertTrue(can_handle)
        
        can_handle = self.supervisor._agent_can_handle_task(self.ml_agent, data_task)
        self.assertFalse(can_handle)
    
    def test_task_assignment(self):
        """Test task assignment to agents."""
        # Register agent
        self.supervisor.register_agent(self.data_agent)
        
        # Submit task
        task = create_data_processing_task(
            task_id="assign-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        self.supervisor.submit_task(task)
        
        # Process task queue
        self.supervisor._process_task_queue()
        
        # Check if task was assigned
        self.assertEqual(task.status, "assigned")
        self.assertEqual(task.assigned_agent, self.data_agent.id)
        self.assertEqual(self.data_agent.status, AgentStatus.BUSY)
        self.assertEqual(self.data_agent.current_task, task.id)
    
    def test_task_completion(self):
        """Test task completion functionality."""
        # Register agent and submit task
        self.supervisor.register_agent(self.data_agent)
        task = create_data_processing_task(
            task_id="complete-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        self.supervisor.submit_task(task)
        
        # Assign task
        self.supervisor._process_task_queue()
        
        # Complete task
        result = {"status": "success", "output": "test_result"}
        success = self.supervisor.complete_task(task.id, result, self.data_agent.id)
        
        self.assertTrue(success)
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.result, result)
        self.assertEqual(self.data_agent.status, AgentStatus.IDLE)
        self.assertIsNone(self.data_agent.current_task)
        self.assertIn(task, self.supervisor.completed_tasks)
    
    def test_task_failure(self):
        """Test task failure handling."""
        # Register agent and submit task
        self.supervisor.register_agent(self.data_agent)
        task = create_data_processing_task(
            task_id="fail-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        self.supervisor.submit_task(task)
        
        # Assign task
        self.supervisor._process_task_queue()
        
        # Fail task - set retry count to max to ensure it fails permanently
        task.metadata["retry_count"] = 3  # Max retries
        error = "Test error"
        success = self.supervisor.fail_task(task.id, error, self.data_agent.id)
        
        self.assertTrue(success)
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.error, error)
        self.assertEqual(self.data_agent.status, AgentStatus.ERROR)
        self.assertIsNone(self.data_agent.current_task)
    
    def test_system_status(self):
        """Test system status reporting."""
        # Register agents
        self.supervisor.register_agent(self.data_agent)
        self.supervisor.register_agent(self.ml_agent)
        
        # Submit tasks
        task1 = create_data_processing_task(
            task_id="status-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        task2 = create_data_processing_task(
            task_id="status-task-002",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output"
        )
        
        self.supervisor.submit_task(task1)
        self.supervisor.submit_task(task2)
        
        # Get system status after task processing
        status = self.supervisor.get_system_status()
        
        # Verify status - note that tasks may be automatically assigned
        self.assertEqual(status["total_agents"], 2)
        # Available agents should be 1 since only one task was assigned (one agent is busy)
        self.assertEqual(status["available_agents"], 1)
        # Pending tasks should be 1 since one task couldn't be assigned
        self.assertEqual(status["pending_tasks"], 1)
        # Assigned tasks should be 1
        self.assertEqual(status["assigned_tasks"], 1)
        self.assertEqual(status["completed_tasks"], 0)
        self.assertEqual(status["failed_tasks"], 0)
        self.assertEqual(status["system_health"], "healthy")
    
    def test_utility_functions(self):
        """Test utility functions for creating common task types."""
        # Test data processing task creation
        data_task = create_data_processing_task(
            task_id="util-task-001",
            data_source="/test/data",
            processing_logic="test_logic",
            output_destination="/test/output",
            priority=TaskPriority.HIGH
        )
        
        self.assertEqual(data_task.name, "Data Processing: /test/data")
        self.assertEqual(data_task.priority, TaskPriority.HIGH)
        self.assertIn("data_processing", data_task.required_capabilities)
        self.assertIn("spark", data_task.required_capabilities)
        
        # Test ML training task creation
        ml_task = create_ml_training_task(
            task_id="util-task-002",
            model_name="Test Model",
            training_data="/test/data",
            hyperparameters={"param1": "value1"}
        )
        
        self.assertEqual(ml_task.name, "ML Training: Test Model")
        self.assertEqual(ml_task.priority, TaskPriority.HIGH)
        self.assertIn("ml_training", ml_task.required_capabilities)
        
        # Test data quality task creation
        quality_task = create_data_quality_check_task(
            task_id="util-task-003",
            dataset_path="/test/data",
            quality_checks=["check1", "check2"]
        )
        
        self.assertEqual(quality_task.name, "Data Quality Check: /test/data")
        self.assertEqual(quality_task.priority, TaskPriority.MEDIUM)
        self.assertIn("data_quality", quality_task.required_capabilities)

def run_basic_tests():
    """Run basic tests without requiring Databricks connection."""
    print("🧪 Running Multi-Agent Supervisor Basic Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestMultiAgentSupervisor)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✅ All tests passed!")
        return True
    else:
        print(f"❌ {len(result.failures)} tests failed")
        print(f"❌ {len(result.errors)} tests had errors")
        return False

if __name__ == "__main__":
    # Run tests
    success = run_basic_tests()
    
    if success:
        print("\n🚀 Multi-Agent Supervisor is ready to use!")
        print("💡 Next steps:")
        print("   1. Set up your Databricks environment")
        print("   2. Run the demo: python multi_agent_demo.py")
        print("   3. Explore the configuration options")
    else:
        print("\n⚠️ Some tests failed. Please check the implementation.")
        sys.exit(1)
