#!/usr/bin/env python3
"""
Debug script to test the multi-agent supervisor endpoint directly
"""

import os
from model_serving_utils import query_endpoint, query_endpoint_stream
import json

# Set the endpoint
SERVING_ENDPOINT = "mas-6c04fa76-endpoint"

def test_endpoint():
    """Test the endpoint with a simple query"""
    
    print(f"Testing endpoint: {SERVING_ENDPOINT}")
    print("=" * 50)
    
    # Test messages
    messages = [
        {"role": "user", "content": "what is S&P500"}
    ]
    
    print("Input messages:")
    print(json.dumps(messages, indent=2))
    print("\n" + "=" * 50)
    
    # Test non-streaming first
    print("Testing NON-STREAMING response:")
    try:
        response_messages, request_id = query_endpoint(
            endpoint_name=SERVING_ENDPOINT,
            messages=messages,
            return_traces=False
        )
        
        print(f"Request ID: {request_id}")
        print("Response messages:")
        for i, msg in enumerate(response_messages):
            print(f"Message {i}:")
            print(json.dumps(msg, indent=2))
            
    except Exception as e:
        print(f"Error in non-streaming: {e}")
    
    print("\n" + "=" * 50)
    
    # Test streaming
    print("Testing STREAMING response:")
    try:
        for event in query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=messages,
            return_traces=False
        ):
            print("Event received:")
            print(json.dumps(event, indent=2))
            print("-" * 30)
            
    except Exception as e:
        print(f"Error in streaming: {e}")

if __name__ == "__main__":
    test_endpoint()
