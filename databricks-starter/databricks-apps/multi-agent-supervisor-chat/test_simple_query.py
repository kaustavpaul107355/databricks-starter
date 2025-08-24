#!/usr/bin/env python3
"""
Test with the original simple query that was working
"""

import os
from model_serving_utils import query_endpoint_stream

# Set the endpoint
SERVING_ENDPOINT = "mas-6c04fa76-endpoint"

def test_simple_query():
    """Test with the original simple query"""
    print(f"Testing simple query on endpoint: {SERVING_ENDPOINT}")
    print("=" * 60)
    
    # Original simple query that was working
    messages = [
        {"role": "user", "content": "what is S&P500"}
    ]
    
    print("Input messages:")
    print(f"  {messages}")
    print()
    
    try:
        print("🔄 Starting streaming call with simple query...")
        
        stream_response = query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=messages,
            return_traces=False
        )
        
        print("✅ Stream response object created successfully")
        print("🔄 Now iterating through stream...")
        
        event_count = 0
        for raw_event in stream_response:
            event_count += 1
            print(f"\n📨 Event #{event_count} received:")
            print(f"   Type: {raw_event.get('type', 'unknown')}")
            
            if raw_event.get("type") == "response.output_text.delta":
                delta = raw_event.get("delta", "")
                print(f"   Delta text: '{delta[:100]}{'...' if len(delta) > 100 else ''}'")
            elif raw_event.get("type") == "response.output_item.done":
                print(f"   Final message received")
            elif raw_event.get("type") == "error":
                print(f"   ERROR: {raw_event.get('message', 'Unknown error')}")
                print(f"   Error code: {raw_event.get('code', 'none')}")
            else:
                print(f"   Raw event: {str(raw_event)[:200]}...")
            
            # Limit to first 30 events
            if event_count >= 30:
                print(f"\n⚠️  Reached event limit ({event_count}). Stopping.")
                break
        
        print(f"\n✅ Streaming completed. Total events: {event_count}")
        return True
        
    except Exception as e:
        print(f"❌ Error during streaming: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Testing simple query that was working before...")
    success = test_simple_query()
    if success:
        print("\n✅ Simple query test completed!")
    else:
        print("\n❌ Simple query test failed!")
