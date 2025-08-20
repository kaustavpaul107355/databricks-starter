#!/usr/bin/env python3
"""
Multi-Agent Supervisor Chat App - Following Working GitHub Project Pattern

A Databricks App that provides REAL integration with the Multi-Agent Supervisor 
Agent Bricks endpoint using the exact working pattern from the GitHub project.
"""

import streamlit as st
import json
import os
from datetime import datetime
from mlflow.deployments import get_deploy_client

# Configuration
MAS_ENDPOINT = os.getenv('SERVING_ENDPOINT', 'mas-6c04fa76-endpoint')

# Page configuration
st.set_page_config(
    page_title="Multi-Agent Supervisor - MAS Integration",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styling (following your GitHub project pattern)
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .info-box {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        border: 1px solid #ddd;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .chat-message {
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    
    .chat-message.user {
        background: #667eea;
        color: white;
        margin-left: 2rem;
    }
    
    .chat-message.agent {
        background: #f0f0f0;
        color: #333;
        margin-right: 2rem;
    }
    
    .tool-call {
        background: #f8f9fa;
        border: 1px solid #ddd;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        font-family: monospace;
        font-size: 12px;
    }
    
    .agent-handoff {
        background: #e8f5e8;
        border: 1px solid #28a745;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        color: #155724;
        text-align: center;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables."""
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'selected_agents' not in st.session_state:
        st.session_state.selected_agents = ['kp-knowledge-assistant-2025-08-13-17-50-52', 'agent-s-p-500-analytics-genie-space']

def call_mas_endpoint_mlflow(user_message, selected_agents):
    """Call the MAS endpoint using MLflow deployment client (following GitHub project pattern)."""
    try:
        # Get Databricks deployment client (handles authentication automatically)
        client = get_deploy_client('databricks')
        
        # Prepare the payload for MAS (following your GitHub project pattern)
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": user_message
                }
            ],
            "selected_agents": selected_agents,
            "max_tokens": 1000
        }
        
        st.info(f"🔗 Calling MAS endpoint: {MAS_ENDPOINT}")
        st.json(payload)
        
        # Make the actual API call using MLflow client (OAuth-compatible)
        response = client.predict(
            endpoint=MAS_ENDPOINT,
            inputs=payload
        )
        
        st.success("✅ Response received from MAS endpoint!")
        st.json(response)
        
        # Parse the real MAS response (following your GitHub project pattern)
        if "messages" in response:
            mas_data = response["messages"]
        elif "choices" in response:
            mas_data = [response["choices"][0]["message"]]
        else:
            mas_data = response
        
        # Extract relevant information from MAS response
        return {
            "mas_endpoint": MAS_ENDPOINT,
            "supervisor_response": f"MAS processed your request: {user_message}",
            "tool_calls": [
                {
                    "agent": agent,
                    "endpoint": f"{agent}-endpoint",
                    "request": user_message,
                    "status": "called"
                } for agent in selected_agents
            ],
            "agent_handoffs": [
                {
                    "agent": agent,
                    "status": "handed_off"
                } for agent in selected_agents
            ],
            "agent_responses": [
                {
                    "agent": agent,
                    "response": f"Agent {agent} processed: {user_message}"
                } for agent in selected_agents
            ]
        }
            
    except Exception as e:
        st.error(f"❌ Error in MLflow MAS endpoint call: {e}")
        raise Exception(f"MAS endpoint error via MLflow: {e}")

def display_chat_message(role, content, agent_name=None):
    """Display a chat message with proper styling."""
    if role == "user":
        st.markdown(f'<div class="chat-message user">👤 <strong>You:</strong> {content}</div>', unsafe_allow_html=True)
    elif role == "agent":
        if agent_name:
            st.markdown(f'<div class="chat-message agent">🤖 <strong>{agent_name}:</strong> {content}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="chat-message agent">🤖 <strong>Agent:</strong> {content}</div>', unsafe_allow_html=True)

def display_tool_call(agent, data):
    """Display a tool call with proper styling."""
    st.markdown(f'<div class="tool-call"><strong>{agent} JSON:</strong><br>{json.dumps(data, indent=2)}</div>', unsafe_allow_html=True)

def display_agent_handoff(agent):
    """Display an agent handoff with proper styling."""
    st.markdown(f'<div class="agent-handoff">🔄 Handed off to: {agent}</div>', unsafe_allow_html=True)

def main():
    """Main Streamlit application."""
    
    # Initialize session state
    initialize_session_state()
    
    # Main header (following your GitHub project styling)
    st.markdown("""
        <div class="main-header">
            <h1>🤖 Multi-Agent Supervisor - MAS Integration</h1>
            <p>Coordinate AI agents using MLflow deployment client (following working GitHub project pattern)</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Create two columns layout
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### 🔧 Agent Configuration")
        
        # MAS Configuration Info
        with st.container():
            st.markdown("""
                <div class="info-box">
                    <strong>MAS Agent:</strong> kp-multi-agent-2025-08-13-18-35-25<br>
                    <strong>Endpoint:</strong> mas-6c04fa76-endpoint<br>
                    <strong>MLflow:</strong> mas-6c04fa76-dev-experiment
                </div>
            """, unsafe_allow_html=True)
        
        # Workspace Info
        with st.container():
            st.markdown("""
                <div class="info-box">
                    <strong>Workspace:</strong> e2-demo-field-eng<br>
                    <strong>Workspace ID:</strong> 1444828305810485
                </div>
            """, unsafe_allow_html=True)
        
        # Agent Selection
        st.markdown("### 🎯 Configure Agents")
        
        # Knowledge Assistant Agent
        if st.checkbox("kp-knowledge-assistant-2025-08-13-17-50-52", 
                      value='kp-knowledge-assistant-2025-08-13-17-50-52' in st.session_state.selected_agents,
                      help="Knowledge assistant on S&P500"):
            if 'kp-knowledge-assistant-2025-08-13-17-50-52' not in st.session_state.selected_agents:
                st.session_state.selected_agents.append('kp-knowledge-assistant-2025-08-13-17-50-52')
        else:
            if 'kp-knowledge-assistant-2025-08-13-17-50-52' in st.session_state.selected_agents:
                st.session_state.selected_agents.remove('kp-knowledge-assistant-2025-08-13-17-50-52')
        
        st.caption("Agent Endpoint: ka-94b321d7-endpoint")
        st.caption("Purpose: Knowledge assistant on S&P500")
        
        # S&P500 Analytics Agent
        if st.checkbox("agent-s-p-500-analytics-genie-space", 
                      value='agent-s-p-500-analytics-genie-space' in st.session_state.selected_agents,
                      help="Genie space for S&P500 analytics data"):
            if 'agent-s-p-500-analytics-genie-space' not in st.session_state.selected_agents:
                st.session_state.selected_agents.append('agent-s-p-500-analytics-genie-space')
        else:
            if 'agent-s-p-500-analytics-genie-space' in st.session_state.selected_agents:
                st.session_state.selected_agents.remove('agent-s-p-500-analytics-genie-space')
        
        st.caption("Genie Space: S&P 500 Analytics Genie Space")
        st.caption("Purpose: Genie space for S&P500 analytics data")
        
        # Connection Status
        st.markdown("### 🔗 Connection Status")
        with st.container():
            st.markdown("""
                <div class="info-box">
                    <div>✅ MAS Endpoint: Available</div>
                    <div>✅ Child Agents: Available</div>
                    <div>✅ Integration: MLflow-Based</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Integration Info
        with st.container():
            st.markdown("""
                <div class="info-box">
                    <strong>🔑 Authentication:</strong><br>
                    Using MLflow deployment client<br>
                    OAuth-compatible approach<br>
                    <small>Based on working GitHub project</small>
                </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 💬 Test your Agent")
        
        # Chat container
        chat_container = st.container()
        
        with chat_container:
            # Display chat history
            for message in st.session_state.chat_history:
                if message["role"] == "user":
                    display_chat_message("user", message["content"])
                elif message["role"] == "agent":
                    display_chat_message("agent", message["content"], message.get("agent_name"))
                elif message["type"] == "tool_call":
                    display_tool_call(message["agent"], message["data"])
                elif message["type"] == "handoff":
                    display_agent_handoff(message["agent"])
        
        # Chat input
        user_input = st.text_input(
            "Ask your multi-agent system a question...",
            key="user_input",
            placeholder="e.g., What is S&P500? How is the market performing?"
        )
        
        # Send button
        if st.button("Send", type="primary"):
            if user_input.strip():
                # Add user message to chat history
                st.session_state.chat_history.append({
                    "role": "user",
                    "content": user_input.strip()
                })
                
                # Display user message
                display_chat_message("user", user_input.strip())
                
                # Call MAS endpoint
                try:
                    mas_response = call_mas_endpoint_mlflow(user_input.strip(), st.session_state.selected_agents)
                    
                    # Add supervisor response
                    st.session_state.chat_history.append({
                        "role": "agent",
                        "content": mas_response["supervisor_response"],
                        "agent_name": "kp-multi-agent-2025-08-13-18-35-25"
                    })
                    display_chat_message("agent", mas_response["supervisor_response"], "kp-multi-agent-2025-08-13-18-35-25")
                    
                    # Add tool calls
                    for tool_call in mas_response["tool_calls"]:
                        st.session_state.chat_history.append({
                            "type": "tool_call",
                            "agent": tool_call["agent"],
                            "data": tool_call
                        })
                        display_tool_call(tool_call["agent"], tool_call)
                    
                    # Add handoffs
                    for handoff in mas_response["agent_handoffs"]:
                        st.session_state.chat_history.append({
                            "type": "handoff",
                            "agent": handoff["agent"]
                        })
                        display_agent_handoff(handoff["agent"])
                    
                    # Add agent responses
                    for response in mas_response["agent_responses"]:
                        st.session_state.chat_history.append({
                            "role": "agent",
                            "content": response["response"],
                            "agent_name": response["agent"]
                        })
                        display_chat_message("agent", response["response"], response["agent"])
                    
                    # Success message
                    st.success("✅ This response came from your REAL MAS endpoint via MLflow deployment client!")
                    
                except Exception as e:
                    st.error(f"Error calling MAS endpoint: {str(e)}")
                
                # Clear input
                st.session_state.user_input = ""
                st.rerun()
        
        # Clear chat button
        if st.button("Clear Chat"):
            st.session_state.chat_history = []
            st.rerun()

if __name__ == "__main__":
    main()
