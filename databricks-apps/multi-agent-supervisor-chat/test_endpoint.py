#!/usr/bin/env python3
"""
Test script to verify connection to the Multi-Agent Supervisor endpoint.
Run this script to test if your endpoint is accessible and responding.
"""

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_endpoint_connection():
    """Test the connection to the multi-agent supervisor endpoint"""
    
    # Get configuration
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    serving_endpoint = os.getenv("SERVING_ENDPOINT", "mas-6c04fa76-endpoint")
    
    print("🔍 Testing Multi-Agent Supervisor Endpoint Connection")
    print("=" * 60)
    
    # Validate configuration
    if not databricks_host:
        print("❌ DATABRICKS_HOST not configured")
        return False
    
    if not databricks_token:
        print("❌ DATABRICKS_TOKEN not configured")
        return False
    
    print(f"✅ Host: {databricks_host}")
    print(f"✅ Endpoint: {serving_endpoint}")
    print(f"✅ Token: {'*' * (len(databricks_token) - 4) + databricks_token[-4:] if len(databricks_token) > 4 else '***'}")
    
    # Test endpoint URL
    url = f"https://{databricks_host}/api/2.0/serving-endpoints/{serving_endpoint}/invocations"
    print(f"\n🔗 Testing URL: {url}")
    
    # Prepare test payload
    test_messages = [
        {"role": "user", "content": "Hello! This is a test message to verify the endpoint is working."}
    ]
    
    payload = {
        "dataframe_records": [
            {
                "messages": test_messages,
                "stream": False
            }
        ]
    }
    
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    print("\n📤 Sending test request...")
    
    try:
        # Send test request
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"📥 Response Status: {response.status_code}")
        print(f"📥 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ Success! Endpoint is responding")
            
            try:
                response_data = response.json()
                print(f"📊 Response Data: {json.dumps(response_data, indent=2)}")
            except json.JSONDecodeError:
                print(f"📝 Response Text: {response.text[:500]}...")
            
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"📝 Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("⏰ Timeout: Request took too long")
        return False
    except requests.exceptions.ConnectionError:
        print("🔌 Connection Error: Could not connect to the endpoint")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {str(e)}")
        return False
    
    print("\n" + "=" * 60)
    print("🎉 Endpoint test completed!")
    return True

def test_endpoint_info():
    """Test getting endpoint information"""
    
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    serving_endpoint = os.getenv("SERVING_ENDPOINT", "mas-6c04fa76-endpoint")
    
    if not all([databricks_host, databricks_token]):
        print("❌ Configuration incomplete for endpoint info test")
        return
    
    print("\n🔍 Getting endpoint information...")
    
    url = f"https://{databricks_host}/api/2.0/serving-endpoints/{serving_endpoint}"
    
    headers = {
        "Authorization": f"Bearer {databricks_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            endpoint_info = response.json()
            print("✅ Endpoint Information:")
            print(f"   Name: {endpoint_info.get('name', 'N/A')}")
            print(f"   State: {endpoint_info.get('state', 'N/A')}")
            print(f"   Config: {endpoint_info.get('config', 'N/A')}")
        else:
            print(f"❌ Could not get endpoint info: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error getting endpoint info: {str(e)}")

if __name__ == "__main__":
    print("🚀 Multi-Agent Supervisor Endpoint Test")
    print("=" * 60)
    
    # Test basic connection
    success = test_endpoint_connection()
    
    if success:
        # Test endpoint information
        test_endpoint_info()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ All tests passed! Your endpoint is ready to use.")
        print("\n📱 Next steps:")
        print("1. Run the chat app: ./run_local.sh")
        print("2. Or deploy to Databricks: ./deploy.sh")
    else:
        print("❌ Some tests failed. Please check your configuration.")
        print("\n🔧 Troubleshooting:")
        print("1. Verify your .env file is configured correctly")
        print("2. Check that your endpoint is running")
        print("3. Ensure your token has the necessary permissions")
