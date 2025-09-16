#!/usr/bin/env python3
"""
Test script for the Zerobus Delta App endpoints
Run this script to test the FastAPI endpoints locally
"""

import requests
import json
from datetime import datetime
import time

# Base URL for the FastAPI server
BASE_URL = "http://localhost:8000"

def test_health_endpoint():
    """Test the health endpoint"""
    print("🔍 Testing health endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_structured_endpoint():
    """Test the structured payload processing endpoint"""
    print("\n🔍 Testing structured payload endpoint...")
    
    payload = {
        "items": [
            {
                "id": "test-item-1",
                "data": {
                    "name": "John Doe",
                    "email": "john@example.com",
                    "age": 30,
                    "department": "Engineering"
                },
                "category": "users",
                "timestamp": datetime.now().isoformat()
            },
            {
                "data": {
                    "product_id": "prod-123",
                    "quantity": 5,
                    "price": 29.99
                },
                "category": "orders"
            },
            {
                "data": {
                    "event_type": "page_view",
                    "user_id": "user-456",
                    "page": "/dashboard",
                    "duration": 45
                },
                "category": "analytics"
            }
        ],
        "source": "test_script",
        "batch_id": f"test_batch_{int(time.time())}"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/process",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_simple_endpoint():
    """Test the simple payload processing endpoint"""
    print("\n🔍 Testing simple payload endpoint...")
    
    payload = {
        "message": "Hello from test script",
        "data": [1, 2, 3, 4, 5],
        "metadata": {
            "test": True,
            "timestamp": datetime.now().isoformat()
        }
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/process-simple",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_error_handling():
    """Test error handling with invalid payload"""
    print("\n🔍 Testing error handling...")
    
    # Test with empty items array (should fail validation)
    invalid_payload = {
        "items": [],  # This should trigger validation error
        "source": "test"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/process",
            json=invalid_payload,
            headers={"Content-Type": "application/json"}
        )
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 400  # Should return 400 for validation error
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting endpoint tests for Zerobus Delta App")
    print("=" * 50)
    
    # Check if server is running
    try:
        requests.get(f"{BASE_URL}/health", timeout=5)
    except:
        print("❌ Server is not running!")
        print("Please start the server with: uvicorn app:app --reload --host 0.0.0.0 --port 8000")
        return
    
    results = []
    
    # Run tests
    results.append(("Health Endpoint", test_health_endpoint()))
    results.append(("Structured Endpoint", test_structured_endpoint()))
    results.append(("Simple Endpoint", test_simple_endpoint()))
    results.append(("Error Handling", test_error_handling()))
    
    # Print results
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    print("=" * 50)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    print(f"\nOverall: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("🎉 All tests passed! The FastAPI server is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the server logs for details.")

if __name__ == "__main__":
    main()
