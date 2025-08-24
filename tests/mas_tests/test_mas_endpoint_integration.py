#!/usr/bin/env python3
"""
Test script for MAS Endpoint Integration

This script tests the integration between the Multi-Agent Supervisor and the 
mas-6c04fa76-endpoint, verifying that the connection and basic functionality work correctly.
"""

import sys
import os
import unittest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from mas_endpoint_client import MASEndpointClient, create_mas_client
from multi_agent_supervisor import MultiAgentSupervisor, create_data_processing_task, TaskPriority

class TestMASEndpointIntegration(unittest.TestCase):
    """Test cases for MAS Endpoint Integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock objects
        self.mock_workspace_client = Mock()
        self.mock_spark_session = Mock()
        
        # Mock endpoint response
        self.mock_endpoint = Mock()
        self.mock_endpoint.state.value = "READY"
        self.mock_endpoint.creation_timestamp = datetime.now()
        self.mock_endpoint.last_updated_timestamp = datetime.now()
        
        self.mock_workspace_client.serving_endpoints.get.return_value = self.mock_endpoint
        self.mock_workspace_client.config.host = "https://test-workspace.cloud.databricks.com"
        self.mock_workspace_client.config.token = "test-token"
    
    def test_mas_endpoint_client_initialization(self):
        """Test MAS endpoint client initialization."""
        try:
            client = MASEndpointClient(self.mock_workspace_client, "mas-6c04fa76-endpoint")
            self.assertIsNotNone(client)
            self.assertEqual(client.endpoint_name, "mas-6c04fa76-endpoint")
            self.assertIsNotNone(client.endpoint_url)
            print("✅ MAS endpoint client initialization test passed")
        except Exception as e:
            print(f"❌ MAS endpoint client initialization test failed: {e}")
            raise
    
    def test_mas_endpoint_status(self):
        """Test getting MAS endpoint status."""
        try:
            client = MASEndpointClient(self.mock_workspace_client, "mas-6c04fa76-endpoint")
            status = client.get_endpoint_status()
            
            self.assertIsInstance(status, dict)
            self.assertEqual(status["endpoint_name"], "mas-6c04fa76-endpoint")
            self.assertEqual(status["state"], "READY")
            print("✅ MAS endpoint status test passed")
        except Exception as e:
            print(f"❌ MAS endpoint status test failed: {e}")
            raise
    
    @patch('requests.post')
    def test_mas_endpoint_task_submission(self, mock_post):
        """Test submitting tasks to MAS endpoint."""
        try:
            # Mock successful HTTP response
            mock_response = Mock()
            mock_response.json.return_value = {
                "predictions": ["Task submitted successfully"],
                "status": "success"
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response
            
            client = MASEndpointClient(self.mock_workspace_client, "mas-6c04fa76-endpoint")
            
            task_data = {
                "action": "submit_task",
                "task": {
                    "description": "Test data processing task",
                    "required_capabilities": ["data_processing", "python"],
                    "priority": "high"
                }
            }
            
            response = client.send_task_to_endpoint(task_data)
            
            self.assertTrue(response["success"])
            self.assertIn("result", response)
            print("✅ MAS endpoint task submission test passed")
        except Exception as e:
            print(f"❌ MAS endpoint task submission test failed: {e}")
            raise
    
    def test_multi_agent_supervisor_with_mas_endpoint(self):
        """Test Multi-Agent Supervisor with MAS endpoint integration."""
        try:
            supervisor = MultiAgentSupervisor(
                self.mock_workspace_client, 
                self.mock_spark_session, 
                "mas-6c04fa76-endpoint"
            )
            
            # Verify MAS client is initialized
            self.assertIsNotNone(supervisor.mas_client)
            self.assertEqual(supervisor.mas_client.endpoint_name, "mas-6c04fa76-endpoint")
            print("✅ Multi-Agent Supervisor with MAS endpoint test passed")
        except Exception as e:
            print(f"❌ Multi-Agent Supervisor with MAS endpoint test failed: {e}")
            raise
    
    @patch('requests.post')
    def test_hybrid_task_processing(self, mock_post):
        """Test hybrid task processing (local + MAS endpoint)."""
        try:
            # Mock successful HTTP response for MAS endpoint
            mock_response = Mock()
            mock_response.json.return_value = {
                "predictions": ["Task processed successfully"],
                "status": "success"
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response
            
            supervisor = MultiAgentSupervisor(
                self.mock_workspace_client, 
                self.mock_spark_session, 
                "mas-6c04fa76-endpoint"
            )
            
            # Create a test task
            task = create_data_processing_task(
                task_id="hybrid-test-001",
                data_source="/test/data",
                processing_logic="test_processing",
                output_destination="/test/output",
                priority=TaskPriority.HIGH
            )
            
            # Test hybrid processing (should use MAS endpoint since no local agents)
            strategy = supervisor.hybrid_task_processing(task)
            
            # Should use MAS endpoint since no local agents are registered
            self.assertEqual(strategy, "mas_endpoint")
            self.assertTrue(task.metadata.get("submitted_to_mas", False))
            print("✅ Hybrid task processing test passed")
        except Exception as e:
            print(f"❌ Hybrid task processing test failed: {e}")
            raise
    
    def test_mas_endpoint_health_check(self):
        """Test MAS endpoint health check functionality."""
        try:
            supervisor = MultiAgentSupervisor(
                self.mock_workspace_client, 
                self.mock_spark_session, 
                "mas-6c04fa76-endpoint"
            )
            
            # Mock health check response
            with patch.object(supervisor.mas_client, 'health_check') as mock_health:
                mock_health.return_value = {
                    "endpoint_health": {"success": True},
                    "endpoint_status": {"state": "READY"},
                    "overall_health": "healthy"
                }
                
                status = supervisor.get_mas_endpoint_status()
                self.assertTrue(status.get("available", False))
                print("✅ MAS endpoint health check test passed")
        except Exception as e:
            print(f"❌ MAS endpoint health check test failed: {e}")
            raise
    
    def test_mas_endpoint_error_handling(self):
        """Test error handling for MAS endpoint failures."""
        try:
            # Test with invalid endpoint name
            with patch('src.mas_endpoint_client.MASEndpointClient.__init__') as mock_init:
                mock_init.side_effect = Exception("Endpoint not found")
                
                supervisor = MultiAgentSupervisor(
                    self.mock_workspace_client, 
                    self.mock_spark_session, 
                    "invalid-endpoint"
                )
                
                # Should continue without MAS client
                self.assertIsNone(supervisor.mas_client)
                
                # Should return error status
                status = supervisor.get_mas_endpoint_status()
                self.assertFalse(status.get("available", False))
                self.assertIn("error", status)
                print("✅ MAS endpoint error handling test passed")
        except Exception as e:
            print(f"❌ MAS endpoint error handling test failed: {e}")
            raise

def run_mas_integration_tests():
    """Run MAS endpoint integration tests."""
    print("🧪 Running MAS Endpoint Integration Tests")
    print("=" * 60)
    
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestMASEndpointIntegration)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✅ All MAS endpoint integration tests passed!")
        return True
    else:
        print(f"❌ {len(result.failures)} tests failed")
        print(f"❌ {len(result.errors)} tests had errors")
        return False

def test_mas_endpoint_connection():
    """Test actual connection to MAS endpoint (requires real Databricks environment)."""
    print("🔗 Testing Real MAS Endpoint Connection")
    print("=" * 60)
    
    # Check if environment variables are set
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("⚠️ Skipping real endpoint test - missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.connect import DatabricksSession
        
        # Initialize real connections
        workspace_client = WorkspaceClient(
            host=os.environ['DATABRICKS_HOST'],
            token=os.environ['DATABRICKS_TOKEN']
        )
        
        # Test MAS endpoint client
        print("🔄 Testing MAS endpoint client...")
        client = MASEndpointClient(workspace_client, "mas-6c04fa76-endpoint")
        
        # Test endpoint status
        print("🔄 Checking endpoint status...")
        status = client.get_endpoint_status()
        print(f"   Endpoint state: {status.get('state', 'unknown')}")
        
        # Test health check
        print("🔄 Performing health check...")
        health = client.health_check()
        print(f"   Health status: {health.get('overall_health', 'unknown')}")
        
        if health.get('overall_health') == 'healthy':
            print("✅ Real MAS endpoint connection test passed!")
            return True
        else:
            print("⚠️ MAS endpoint is not healthy")
            return False
            
    except Exception as e:
        print(f"❌ Real endpoint connection test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 MAS Endpoint Integration Test Suite")
    print("=" * 70)
    
    # Run unit tests
    unit_tests_passed = run_mas_integration_tests()
    
    # Run real endpoint test if requested
    print("\nDo you want to test the real MAS endpoint connection?")
    print("(This requires valid Databricks credentials)")
    test_real = input("Test real endpoint? (y/N): ").strip().lower()
    
    real_test_passed = True
    if test_real in ['y', 'yes']:
        real_test_passed = test_mas_endpoint_connection()
    
    # Final summary
    print("\n" + "=" * 70)
    if unit_tests_passed and real_test_passed:
        print("🎉 All MAS endpoint integration tests completed successfully!")
        print("\n💡 Next steps:")
        print("   1. Run the MAS endpoint demo: python multi_agent_demo.py (option 3)")
        print("   2. Integrate MAS endpoint into your workflows")
        print("   3. Monitor endpoint performance and usage")
    else:
        print("⚠️ Some tests failed. Please check the configuration and try again.")
        sys.exit(1)
