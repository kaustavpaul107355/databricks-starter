#!/usr/bin/env python3
"""
Test script to verify response handling logic
"""

import json

# Simulate the actual response events we received from the debug script
test_events = [
    {
        "type": "response.output_text.delta",
        "item_id": "aa68883b-1432-447b-a772-0fd0d6cee9eb",
        "delta": "The S&P 500 is a stock market index comprising 500 leading U.S. companies",
        "id": "346f5a26-2f5d-499f-be13-f17395131695"
    },
    {
        "type": "response.output_text.delta",
        "item_id": "aa68883b-1432-447b-a772-0fd0d6cee9eb",
        "delta": ", representing about 80% of the U.S. equity market capitalization",
        "id": "346f5a26-2f5d-499f-be13-f17395131695"
    },
    {
        "type": "response.output_item.done",
        "item": {
            "id": "aa68883b-1432-447b-a772-0fd0d6cee9eb",
            "content": [
                {
                    "text": "The S&P 500 is a stock market index comprising 500 leading U.S. companies, representing about 80% of the U.S. equity market capitalization and over 50% of the global equity market.",
                    "type": "output_text"
                }
            ],
            "role": "assistant",
            "type": "message"
        },
        "id": "346f5a26-2f5d-499f-be13-f17395131695"
    }
]

def test_response_handling():
    """Test the response handling logic"""
    print("Testing response handling logic...")
    print("=" * 50)
    
    all_messages = []
    
    for raw_event in test_events:
        print(f"Processing event: {raw_event['type']}")
        
        if raw_event.get("type") == "response.output_text.delta":
            delta_text = raw_event.get("delta", "")
            if delta_text:
                # Find or create the current assistant message
                current_assistant_msg = None
                for msg in all_messages:
                    if msg.get("role") == "assistant" and not msg.get("tool_calls"):
                        current_assistant_msg = msg
                        break
                
                if not current_assistant_msg:
                    current_assistant_msg = {
                        "role": "assistant",
                        "content": ""
                    }
                    all_messages.append(current_assistant_msg)
                
                current_assistant_msg["content"] += delta_text
                print(f"  -> Accumulated delta: '{delta_text}'")
                print(f"  -> Current content: '{current_assistant_msg['content']}'")
        
        elif raw_event.get("type") == "response.output_item.done":
            if "item" in raw_event:
                item = raw_event["item"]
                if item.get("type") == "message":
                    content_parts = item.get("content", [])
                    for content_part in content_parts:
                        if content_part.get("type") == "output_text":
                            text = content_part.get("text", "")
                            if text:
                                final_message = {
                                    "role": "assistant",
                                    "content": text
                                }
                                
                                # Replace any existing assistant message without tool calls
                                for i, msg in enumerate(all_messages):
                                    if msg.get("role") == "assistant" and not msg.get("tool_calls"):
                                        all_messages[i] = final_message
                                        break
                                else:
                                    all_messages.append(final_message)
                                
                                print(f"  -> Final message: '{text}'")
    
    print("\n" + "=" * 50)
    print("Final accumulated messages:")
    for i, msg in enumerate(all_messages):
        print(f"  {i+1}. Role: {msg['role']}")
        print(f"     Content: {msg['content'][:100]}...")
        if msg.get('tool_calls'):
            print(f"     Tool calls: {len(msg['tool_calls'])}")
    
    return all_messages

if __name__ == "__main__":
    result = test_response_handling()
    print(f"\n✅ Test completed. Found {len(result)} messages.")
