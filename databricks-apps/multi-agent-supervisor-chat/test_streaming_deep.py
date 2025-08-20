#!/usr/bin/env python3
"""
Deep dive test to see where streaming is getting stuck
"""

import os
from model_serving_utils import query_endpoint_stream

# Set the endpoint
SERVING_ENDPOINT = "mas-6c04fa76-endpoint"

def test_streaming_deep():
    """Test streaming with detailed logging"""
    print(f"Testing streaming endpoint: {SERVING_ENDPOINT}")
    print("=" * 60)
    
    # Test messages
    messages = [
        {"role": "user", "content": "show me top 10 industry trends for S&P500 for last 5 years"}
    ]
    
    print("Input messages:")
    print(f"  {messages}")
    print()
    
    try:
        print("🔄 Starting streaming call...")
        print("   This should start receiving events...")
        
        # Start the streaming call
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
            print(f"   Event ID: {raw_event.get('id', 'none')}")
            
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
            
            # Limit to first 20 events to avoid infinite loop
            if event_count >= 20:
                print(f"\n⚠️  Reached event limit ({event_count}). Stopping to avoid infinite loop.")
                break
        
        print(f"\n✅ Streaming completed. Total events: {event_count}")
        return True
        
    except Exception as e:
        print(f"❌ Error during streaming: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting deep dive streaming test...")
    success = test_streaming_deep()
    if success:
        print("\n✅ Deep dive test completed successfully!")
    else:
        print("\n❌ Deep dive test failed!")
