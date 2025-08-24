#!/usr/bin/env python3
"""
Test non-streaming endpoint call
"""

import os
from model_serving_utils import query_endpoint

# Set the endpoint
SERVING_ENDPOINT = "mas-6c04fa76-endpoint"

def test_non_streaming():
    """Test the non-streaming endpoint call"""
    print(f"Testing non-streaming endpoint: {SERVING_ENDPOINT}")
    print("=" * 50)
    
    # Test messages
    messages = [
        {"role": "user", "content": "what is S&P500"}
    ]
    
    print("Input messages:")
    print(f"  {messages}")
    print()
    
    try:
        print("Calling endpoint without streaming...")
        response_messages, request_id = query_endpoint(
            endpoint_name=SERVING_ENDPOINT,
            messages=messages,
            return_traces=False
        )
        
        print("✅ Success!")
        print(f"Request ID: {request_id}")
        print(f"Response messages: {len(response_messages)}")
        
        for i, msg in enumerate(response_messages):
            print(f"  Message {i+1}:")
            print(f"    Role: {msg.get('role', 'unknown')}")
            print(f"    Content: {msg.get('content', '')[:200]}...")
            if msg.get('tool_calls'):
                print(f"    Tool calls: {len(msg['tool_calls'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    success = test_non_streaming()
    if success:
        print("\n✅ Non-streaming test passed!")
    else:
        print("\n❌ Non-streaming test failed!")
