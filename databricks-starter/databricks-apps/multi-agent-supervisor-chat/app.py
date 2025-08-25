import logging
import os
import streamlit as st
from model_serving_utils import (
    endpoint_supports_feedback, 
    query_endpoint, 
    query_endpoint_stream, 
    _get_endpoint_task_type,
)
from collections import OrderedDict
from messages import UserMessage, AssistantResponse, render_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVING_ENDPOINT = os.getenv('SERVING_ENDPOINT')
assert SERVING_ENDPOINT, \
    ("Unable to determine serving endpoint to use for chatbot app. If developing locally, "
     "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
     "deploying to a Databricks app, include a serving endpoint resource named "
     "'serving_endpoint' with CAN_QUERY permissions, as described in "
     "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app")

ENDPOINT_SUPPORTS_FEEDBACK = endpoint_supports_feedback(SERVING_ENDPOINT)

def reduce_chat_agent_chunks(chunks):
    """
    Reduce a list of ChatAgentChunk objects corresponding to a particular
    message into a single ChatAgentMessage
    """
    deltas = [chunk.delta for chunk in chunks]
    first_delta = deltas[0]
    result_msg = first_delta
    msg_contents = []
    
    # Accumulate tool calls properly
    tool_call_map = {}  # Map call_id to tool call for accumulation
    
    for delta in deltas:
        # Handle content
        if delta.content:
            msg_contents.append(delta.content)
            
        # Handle tool calls
        if hasattr(delta, 'tool_calls') and delta.tool_calls:
            for tool_call in delta.tool_calls:
                call_id = getattr(tool_call, 'id', None)
                tool_type = getattr(tool_call, 'type', "function")
                function_info = getattr(tool_call, 'function', None)
                if function_info:
                    func_name = getattr(function_info, 'name', "")
                    func_args = getattr(function_info, 'arguments', "")
                else:
                    func_name = ""
                    func_args = ""
                
                if call_id:
                    if call_id not in tool_call_map:
                        # New tool call
                        tool_call_map[call_id] = {
                            "id": call_id,
                            "type": tool_type,
                            "function": {
                                "name": func_name,
                                "arguments": func_args
                            }
                        }
                    else:
                        # Accumulate arguments for existing tool call
                        existing_args = tool_call_map[call_id]["function"]["arguments"]
                        tool_call_map[call_id]["function"]["arguments"] = existing_args + func_args

                        # Update function name if provided
                        if func_name:
                            tool_call_map[call_id]["function"]["name"] = func_name

        # Handle tool call IDs (for tool response messages)
        if hasattr(delta, 'tool_call_id') and delta.tool_call_id:
            result_msg = result_msg.model_copy(update={"tool_call_id": delta.tool_call_id})
    
    # Convert tool call map back to list
    if tool_call_map:
        accumulated_tool_calls = list(tool_call_map.values())
        result_msg = result_msg.model_copy(update={"tool_calls": accumulated_tool_calls})
    
    result_msg = result_msg.model_copy(update={"content": "".join(msg_contents)})
    return result_msg



# --- Init state ---
if "history" not in st.session_state:
    st.session_state.history = []

# Custom Loading Overlay - Using Streamlit Components
import streamlit.components.v1 as components

# Create the custom loader using Streamlit components
components.html("""
<div id="customLoader" style="
    position: fixed;
    top: 15px;
    right: 15px;
    z-index: 99999;
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.98) 0%, rgba(118, 75, 162, 0.98) 100%);
    backdrop-filter: blur(20px);
    border: 3px solid rgba(255, 255, 255, 0.4);
    border-radius: 20px;
    padding: 16px 24px;
    box-shadow: 0 12px 40px rgba(102, 126, 234, 0.6);
    display: flex;
    align-items: center;
    gap: 12px;
    font-size: 1rem;
    font-weight: 700;
    color: white;
    text-shadow: 0 2px 6px rgba(0,0,0,0.4);
    min-width: 180px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
">
    <span>🤖 Processing</span>
    <div style="display: flex; gap: 4px; align-items: center;">
        <div class="dot1" style="
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: white;
            animation: dot-bounce 1.4s ease-in-out infinite both;
            animation-delay: -0.32s;
        "></div>
        <div class="dot2" style="
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: white;
            animation: dot-bounce 1.4s ease-in-out infinite both;
            animation-delay: -0.16s;
        "></div>
        <div class="dot3" style="
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: white;
            animation: dot-bounce 1.4s ease-in-out infinite both;
            animation-delay: 0s;
        "></div>
    </div>
</div>

<style>
@keyframes dot-bounce {
    0%, 80%, 100% {
        transform: scale(0.8);
        opacity: 0.5;
    }
    40% {
        transform: scale(1.2);
        opacity: 1;
    }
}

/* Pulse animation for the container */
#customLoader {
    animation: container-pulse 2s ease-in-out infinite alternate;
}

@keyframes container-pulse {
    0% { 
        opacity: 0.9; 
        transform: translateY(0px) scale(1);
        box-shadow: 0 12px 40px rgba(102, 126, 234, 0.6);
    }
    100% { 
        opacity: 1; 
        transform: translateY(-2px) scale(1.02);
        box-shadow: 0 16px 50px rgba(102, 126, 234, 0.8);
    }
}
</style>

<script>
// Show/hide loader based on Streamlit state
let isProcessing = false;

function showLoader() {
    const loader = document.getElementById('customLoader');
    if (loader) {
        loader.style.display = 'flex';
        console.log('✅ Custom loader visible');
    }
}

function hideLoader() {
    const loader = document.getElementById('customLoader');
    if (loader) {
        loader.style.display = 'none';
        console.log('❌ Custom loader hidden');
    }
}

// Monitor for loading states
function checkForLoading() {
    const spinners = document.querySelectorAll('[data-testid="stSpinner"], .stSpinner');
    const hasSpinners = spinners.length > 0;
    
    if (hasSpinners || isProcessing) {
        showLoader();
    } else {
        hideLoader();
    }
}

// Form submission detection
document.addEventListener('submit', function(e) {
    console.log('🚀 Form submitted - showing loader');
    isProcessing = true;
    showLoader();
    setTimeout(() => {
        isProcessing = false;
        checkForLoading();
    }, 5000);
});

// Check every 100ms
setInterval(checkForLoading, 100);

// AGGRESSIVE BLUE ARC ELIMINATION
function eliminateBlueArcs() {
    // Target all possible spinner selectors
    const spinnerSelectors = [
        '[data-testid="stSpinner"]',
        '.stSpinner',
        'svg[data-testid="stSpinner"]',
        'circle[data-testid="stSpinner"]',
        '.stProgress',
        '[data-testid="stProgress"]',
        '.stChatMessage [data-testid="stSpinner"]',
        'div[data-testid="stChatMessage"] [data-testid="stSpinner"]',
        '*[class*="spinner"]',
        '*[class*="loading"]',
        '*[data-testid*="spinner"]',
        '*[data-testid*="loading"]'
    ];
    
    let eliminated = 0;
    spinnerSelectors.forEach(selector => {
        const elements = document.querySelectorAll(selector);
        elements.forEach(el => {
            if (el && el.style.display !== 'none') {
                el.style.display = 'none';
                el.style.visibility = 'hidden';
                el.style.opacity = '0';
                el.style.position = 'absolute';
                el.style.left = '-9999px';
                el.style.top = '-9999px';
                el.style.zIndex = '-9999';
                eliminated++;
            }
        });
    });
    
    if (eliminated > 0) {
        console.log(`🎯 Eliminated ${eliminated} blue arcs`);
    }
}

// Run elimination every 50ms for aggressive removal
setInterval(eliminateBlueArcs, 50);

// Also run on DOM mutations
const arcObserver = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
        if (mutation.type === 'childList') {
            // Check if any new spinners were added
            mutation.addedNodes.forEach(function(node) {
                if (node.nodeType === 1) { // Element node
                    // Check the node itself
                    if (node.matches && (
                        node.matches('[data-testid="stSpinner"]') ||
                        node.matches('.stSpinner') ||
                        node.matches('*[class*="spinner"]')
                    )) {
                        node.style.display = 'none';
                        console.log('🎯 Intercepted new spinner');
                    }
                    
                    // Check children
                    const childSpinners = node.querySelectorAll && node.querySelectorAll('[data-testid="stSpinner"], .stSpinner, *[class*="spinner"]');
                    if (childSpinners) {
                        childSpinners.forEach(spinner => {
                            spinner.style.display = 'none';
                            console.log('🎯 Intercepted child spinner');
                        });
                    }
                }
            });
        }
    });
});

// Start aggressive observation
if (document.body) {
    arcObserver.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['class', 'data-testid', 'style']
    });
}

// Start with loader visible for testing
showLoader();
console.log('🎯 Custom loader initialized with aggressive blue arc elimination');
</script>
""", height=0)

# Custom CSS for improved color scheme with animations
st.markdown("""
<style>
    /* Improved color scheme with better contrast */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        padding: 1.5rem 1rem;
        border-radius: 20px;
        margin-bottom: 1.5rem;
        color: white;
        text-align: center;
        box-shadow: 0 8px 32px rgba(102,126,234,0.3);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.3);
        transition: all 0.3s ease;
    }
    
    .main-header:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(102,126,234,0.4);
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 800;
        text-shadow: 0 4px 8px rgba(0,0,0,0.4);
        letter-spacing: -0.5px;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.95;
        font-weight: 300;
    }
    
    .endpoint-info {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1.25rem;
        border-radius: 15px;
        color: #2c3e50;
        margin: 0.75rem 0;
        box-shadow: 0 8px 25px rgba(250,112,154,0.3);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.3);
        transition: all 0.3s ease;
    }
    
    .endpoint-info:hover {
        transform: translateY(-1px);
        box-shadow: 0 10px 30px rgba(250,112,154,0.4);
    }
    
    .endpoint-name {
        font-family: 'Courier New', monospace;
        background: rgba(255,255,255,0.25);
        padding: 0.75rem;
        border-radius: 8px;
        font-weight: bold;
        backdrop-filter: blur(5px);
        border: 1px solid rgba(255,255,255,0.2);
        color: #2c3e50;
    }
    
    .glass-card {
        background: rgba(255, 255, 255, 0.15);
        backdrop-filter: blur(15px);
        border: 1px solid rgba(255, 255, 255, 0.25);
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.15);
    }
    
    .status-tile {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.25) 0%, rgba(255, 255, 255, 0.15) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.35);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    
    .status-tile:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
    }
    
    /* Enhanced button animations */
    .stButton > button {
        transition: all 0.3s ease !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(0,0,0,0.2) !important;
    }
    
    /* Chat message animations */
    .chat-message {
        animation: slideIn 0.5s ease-out;
        transition: all 0.3s ease;
    }
    
    .chat-message:hover {
        transform: translateX(5px);
    }
    
    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateX(-20px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }
    
    /* Pulse animation for status indicators */
    .status-pulse {
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.7; }
        100% { opacity: 1; }
    }
    
    /* NUCLEAR OPTION - ELIMINATE ALL BLUE ARCS */
    /* Target ALL possible spinner elements */
    .stSpinner, .stSpinner *, 
    .stProgress, .stProgress *,
    [data-testid="stSpinner"], [data-testid="stSpinner"] *,
    [data-testid="stProgress"], [data-testid="stProgress"] *,
    [data-testid="stStatusWidget"], [data-testid="stStatus"],
    .stAlert[data-baseweb="notification"],
    .stToast, [data-testid="stToast"],
    
    /* Target specific chat message loading indicators */
    .stChatMessage .stSpinner,
    .stChatMessage [data-testid="stSpinner"],
    div[data-testid="stChatMessage"] .stSpinner,
    div[data-testid="stChatMessage"] [data-testid="stSpinner"],
    
    /* Target container-level spinners */
    .element-container .stSpinner,
    .element-container [data-testid="stSpinner"],
    .stApp .stSpinner,
    .stApp [data-testid="stSpinner"],
    .main .stSpinner,
    .main [data-testid="stSpinner"],
    .block-container .stSpinner,
    .block-container [data-testid="stSpinner"],
    
    /* Target specific app areas */
    div[data-testid="stDecoration"],
    .stApp > div[data-testid="stDecoration"],
    .stApp > header + div .stSpinner,
    .stApp > header + div [data-testid="stSpinner"],
    .stApp > div > div > div [data-testid="stSpinner"],
    .stApp header ~ div [data-testid="stSpinner"],
    .stApp [data-testid="stToolbar"] ~ div [data-testid="stSpinner"],
    .stApp > div:first-child [data-testid="stSpinner"],
    div[data-testid="stAppViewContainer"] [data-testid="stSpinner"],
    section[data-testid="stSidebar"] ~ div [data-testid="stSpinner"],
    
    /* Target SVG spinners specifically */
    svg[data-testid="stSpinner"],
    svg.stSpinner,
    circle[data-testid="stSpinner"],
    
    /* Target any remaining loading elements */
    .stStatus, .stStatusWidget, .stAlert[kind="info"],
    
    /* Catch-all for any spinner-like elements */
    *[class*="spinner"], *[class*="loading"], *[class*="progress"],
    *[data-testid*="spinner"], *[data-testid*="loading"], *[data-testid*="progress"] {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
        width: 0 !important;
        height: 0 !important;
        overflow: hidden !important;
        position: absolute !important;
        left: -9999px !important;
        top: -9999px !important;
        z-index: -9999 !important;
    }
    
    /* Custom App Loading Overlay - Prominent Design */
    .custom-loading-overlay {
        position: fixed;
        top: 15px;
        right: 15px;
        z-index: 99999;
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.98) 0%, rgba(118, 75, 162, 0.98) 100%);
        backdrop-filter: blur(20px);
        border: 3px solid rgba(255, 255, 255, 0.4);
        border-radius: 20px;
        padding: 16px 24px;
        box-shadow: 0 12px 40px rgba(102, 126, 234, 0.6);
        animation: custom-loading-pulse 2s ease-in-out infinite alternate;
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 1rem;
        font-weight: 700;
        color: white;
        text-shadow: 0 2px 6px rgba(0,0,0,0.4);
        min-width: 180px;
    }
    
    /* Option 1: Three Animated Dots */
    .loading-dots {
        display: flex;
        gap: 4px;
        align-items: center;
    }
    
    .loading-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: white;
        animation: dot-bounce 1.4s ease-in-out infinite both;
    }
    
    .loading-dot:nth-child(1) { animation-delay: -0.32s; }
    .loading-dot:nth-child(2) { animation-delay: -0.16s; }
    .loading-dot:nth-child(3) { animation-delay: 0s; }
    
    @keyframes dot-bounce {
        0%, 80%, 100% {
            transform: scale(0.8);
            opacity: 0.5;
        }
        40% {
            transform: scale(1.2);
            opacity: 1;
        }
    }
    
    /* Option 2: Three Animated Bars */
    .loading-bars {
        display: flex;
        gap: 3px;
        align-items: end;
        height: 16px;
    }
    
    .loading-bar {
        width: 4px;
        background: white;
        border-radius: 2px;
        animation: bar-grow 1.2s ease-in-out infinite;
    }
    
    .loading-bar:nth-child(1) { 
        animation-delay: -0.4s;
        height: 8px;
    }
    .loading-bar:nth-child(2) { 
        animation-delay: -0.2s;
        height: 12px;
    }
    .loading-bar:nth-child(3) { 
        animation-delay: 0s;
        height: 16px;
    }
    
    @keyframes bar-grow {
        0%, 100% {
            transform: scaleY(0.4);
            opacity: 0.6;
        }
        50% {
            transform: scaleY(1);
            opacity: 1;
        }
    }
    
    @keyframes custom-loading-pulse {
        0% { 
            opacity: 0.8; 
            transform: translateY(0px) scale(1);
            box-shadow: 0 8px 32px rgba(102, 126, 234, 0.4);
        }
        100% { 
            opacity: 1; 
            transform: translateY(-2px) scale(1.02);
            box-shadow: 0 12px 40px rgba(102, 126, 234, 0.6);
        }
    }
    
    @keyframes custom-spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    /* Show custom loading when Streamlit is processing */
    .stApp:has([data-testid="stSpinner"]) .custom-loading-overlay {
        display: flex !important;
    }
    
    /* Alternative: Show during app execution */
    .stApp.stAppLoading .custom-loading-overlay {
        display: flex !important;
    }
    
    /* Show loading overlay when any processing happens */
    .stApp:has(.stSpinner) .custom-loading-overlay,
    .stApp:has([data-testid="stSpinner"]) .custom-loading-overlay {
        display: flex !important;
    }
    
    /* Professional thinking animation */
    .thinking-dots {
        display: inline-block;
        position: relative;
    }
    
    .thinking-dots::after {
        content: '';
        display: inline-block;
        width: 4px;
        height: 4px;
        border-radius: 50%;
        background: #4ecdc4;
        animation: thinking-dots 1.4s infinite ease-in-out both;
        margin-left: 4px;
    }
    
    .thinking-dots::before {
        content: '';
        display: inline-block;
        width: 4px;
        height: 4px;
        border-radius: 50%;
        background: #4ecdc4;
        animation: thinking-dots 1.4s -0.16s infinite ease-in-out both;
        margin-right: 4px;
    }
    
    @keyframes thinking-dots {
        0%, 80%, 100% {
            transform: scale(0);
            opacity: 0.5;
        }
        40% {
            transform: scale(1);
            opacity: 1;
        }
    }
    
    /* Smooth professional thinking animation */
    .thinking-container {
        animation: thinking-glow 2s ease-in-out infinite alternate;
    }
    
    @keyframes thinking-glow {
        0% { 
            opacity: 0.7; 
            transform: translateY(0px);
            box-shadow: 0 4px 15px rgba(78, 205, 196, 0.2);
        }
        100% { 
            opacity: 1; 
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(78, 205, 196, 0.4);
        }
    }
</style>
""", unsafe_allow_html=True)

# Compact Modern Multi-Agent Supervisor System Architecture Banner
st.markdown("""
<div style="
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    border: 2px solid #4ecdc4;
    border-radius: 15px;
    padding: 1.5rem;
    margin: 0.5rem 0;
    box-shadow: 0 8px 25px rgba(78, 205, 196, 0.3);
">
    <div style="text-align: center; margin-bottom: 1rem;">
        <div style="
            font-size: 1.8rem;
            font-weight: 800;
            color: #4ecdc4;
            text-shadow: 0 0 8px rgba(78, 205, 196, 0.5);
            letter-spacing: -0.5px;
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">MULTI-AGENT SUPERVISOR SYSTEM</div>
        <div style="
            font-size: 0.9rem;
            color: rgba(78, 205, 196, 0.9);
            font-weight: 400;
            max-width: 500px;
            margin: 0 auto;
            line-height: 1.3;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Advanced AI orchestration platform for intelligent multi-agent coordination and decision-making</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Top Row - System Functions (Compact)
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #3498db;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(52, 152, 219, 0.3);
    ">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">⚙️</div>
        <div style="
            font-size: 1rem; 
            color: #3498db; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Configuration</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(52, 152, 219, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">System Setup & Control</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #e74c3c;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(231, 76, 60, 0.3);
    ">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">👨‍💼</div>
        <div style="
            font-size: 1rem; 
            color: #e74c3c; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Supervisor</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(231, 76, 60, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Central Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #f39c12;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(243, 156, 18, 0.3);
    ">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">💬</div>
        <div style="
            font-size: 1rem; 
            color: #f39c12; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Communication</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(243, 156, 18, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Agent Interaction</div>
    </div>
    """, unsafe_allow_html=True)

# Central System Hub (Compact)
st.markdown("""
<div style="
    background: linear-gradient(135deg, #8e44ad 0%, #9b59b6 100%);
    border: 3px solid #e8d5ff;
    border-radius: 15px;
    padding: 1.5rem;
    margin: 1rem auto;
    max-width: 400px;
    box-shadow: 0 6px 20px rgba(142, 68, 173, 0.4);
    text-align: center;
">
    <div style="font-size: 2.5rem; margin-bottom: 0.8rem;">📈</div>
    <div style="
        font-size: 1.2rem; 
        font-weight: 700; 
        color: white; 
        margin-bottom: 0.8rem; 
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    ">S&P 500</div>
    <div style="
        font-size: 1.0rem; 
        font-weight: 600; 
        color: rgba(255, 255, 255, 0.95); 
        margin-bottom: 0.6rem;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    ">The Gauge of the U.S. Large-Cap Market</div>
    <div style="
        font-size: 0.85rem; 
        color: rgba(255, 255, 255, 0.9); 
        line-height: 1.5; 
        max-width: 400px; 
        margin: 0 auto;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        text-align: justify;
    ">The S&P 500 is widely regarded as the best single gauge of U.S. large-cap equities. The index includes 500 leading companies spanning all sectors of the U.S. stock market. It covers approximately 80% of the U.S. equity market capitalization and over 50% of the global equity market.</div>
</div>
""", unsafe_allow_html=True)

# Bottom Row - Individual Agents (Compact)
col4, col5, col6 = st.columns(3)
with col4:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #27ae60;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(39, 174, 96, 0.3);
    ">
        <div style="font-size: 1.8rem; margin-bottom: 0.5rem;">👨‍💼</div>
        <div style="
            font-size: 0.95rem; 
            color: #27ae60; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Knowledge Agent</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(39, 174, 96, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Data & Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #e67e22;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(230, 126, 34, 0.3);
    ">
        <div style="font-size: 1.8rem; margin-bottom: 0.5rem;">👨‍💼</div>
        <div style="
            font-size: 0.95rem; 
            color: #e67e22; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Analytics Agent</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(230, 126, 34, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Pattern Analysis</div>
    </div>
    """, unsafe_allow_html=True)

with col6:
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        border: 2px solid #9b59b6;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(155, 89, 182, 0.3);
    ">
        <div style="font-size: 1.8rem; margin-bottom: 0.5rem;">👨‍💼</div>
        <div style="
            font-size: 0.95rem; 
            color: #9b59b6; 
            font-weight: 700; 
            margin-bottom: 0.3rem;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Decision Agent</div>
        <div style="
            font-size: 0.8rem; 
            color: rgba(155, 89, 182, 0.8); 
            font-style: italic;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">Strategic Choices</div>
    </div>
    """, unsafe_allow_html=True)

# Endpoint information with modern styling
st.markdown(f"""
<div class="endpoint-info">
    <div style="text-align: center;">
        <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">🔗 Connected Endpoint</div>
        <div class="endpoint-name">{SERVING_ENDPOINT}</div>
    </div>
</div>
""", unsafe_allow_html=True)



# --- Left Sidebar ---
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 0.75rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 12px; margin-bottom: 0.75rem; box-shadow: 0 4px 15px rgba(102,126,234,0.3); transition: all 0.3s ease;">
        <h4 style="margin: 0; font-size: 1.1rem; font-weight: 600;">🔧 Control Panel</h4>
    </div>
    """, unsafe_allow_html=True)
    
    # Compact User & Endpoint Info
    st.markdown("**👤 User:** kaustav.paul@databricks.com")
    st.markdown(f"**🔗 Endpoint:** `{SERVING_ENDPOINT}`")
    
    st.markdown("---")
    
    # Child Agents Information
    st.markdown("**🤖 Child Agents:**")
    
    # Knowledge Assistant Agent
    st.markdown("""
    <div style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); border: 2px solid #667eea; border-radius: 8px; padding: 0.6rem; margin: 0.3rem 0; font-size: 0.85rem; box-shadow: 0 2px 8px rgba(102,126,234,0.3); transition: all 0.3s ease; cursor: pointer;">
        <strong style="color: #4a5568;">📚 Knowledge Assistant</strong><br>
        <code style="font-size: 0.75rem; color: #2d3748;">kp-knowledge-assistant-2025-08-13-17-50-52</code><br>
        <small style="color: #4a5568;">Endpoint: ka-94b321d7-endpoint</small><br>
        <small style="color: #4a5568;">Purpose: S&P500 knowledge queries</small>
    </div>
    """, unsafe_allow_html=True)
    
    # Genie Space Agent
    st.markdown("""
    <div style="background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%); border: 2px solid #fa709a; border-radius: 8px; padding: 0.6rem; margin: 0.3rem 0; font-size: 0.85rem; box-shadow: 0 2px 8px rgba(250,112,154,0.3); transition: all 0.3s ease; cursor: pointer;">
        <strong style="color: #744210;">📊 Genie Space Analytics</strong><br>
        <code style="font-size: 0.75rem; color: #744210;">agent-s-p-500-analytics-genie-space</code><br>
        <small style="color: #744210;">Space: S&P 500 Analytics Genie Space</small><br>
        <small style="color: #744210;">Purpose: S&P500 analytics data</small>
    </div>
    """, unsafe_allow_html=True)
    
    # Status Tiles - both using the same color scheme
    st.markdown("**📊 Status Overview:**")
    
    # Analytics Status Tile
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 8px; padding: 0.6rem; margin: 0.3rem 0; border: 1px solid rgba(255,255,255,0.4); box-shadow: 0 4px 15px rgba(102,126,234,0.4); transition: all 0.3s ease; cursor: pointer;">
        <div style="font-size: 0.9rem; font-weight: 600; color: white; margin-bottom: 0.4rem; text-align: center;">📊 Analytics</div>
        <div style="font-size: 0.75rem; color: white; margin: 0.2rem 0;">
            <span class="status-pulse" style="color: #a8edea;">🟢</span> Endpoint: Active
        </div>
        <div style="font-size: 0.75rem; color: white; margin: 0.2rem 0;">
            <span style="color: #fed6e3;">📈</span> Queries: Ready
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Insights Status Tile - updated colors
    st.markdown("""
    <div style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); border-radius: 8px; padding: 0.6rem; margin: 0.3rem 0; border: 1px solid rgba(255,255,255,0.4); box-shadow: 0 4px 15px rgba(250,112,154,0.4); transition: all 0.3s ease; cursor: pointer;">
        <div style="font-size: 0.9rem; font-weight: 600; color: #2c3e50; margin-bottom: 0.4rem; text-align: center;">🔍 Insights</div>
        <div style="font-size: 0.75rem; color: #2c3e50; margin: 0.2rem 0;">
            <span class="status-pulse" style="color: #fa709a;">🤖</span> Agents: 2 Active
        </div>
        <div style="font-size: 0.75rem; color: #2c3e50; margin: 0.2rem 0;">
            <span style="color: #fee140;">⚡</span> Status: Ready
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Quick Actions (compact)
    if st.button("🔄 Refresh", use_container_width=True):
        # Refresh without rerun to avoid loading arcs
        pass
    
    if st.button("🗑️ Clear Chat", use_container_width=True):
        st.session_state.history = []
        # Clear without rerun to avoid loading arcs
    
    # Footer (compact)
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.7rem; margin-top: 1rem;">
        Multi-Agent Supervisor<br>
        Interactive Experience
    </div>
    """, unsafe_allow_html=True)

# --- Main Content Area - More Compact ---
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px; padding: 0.75rem; margin: 0.5rem 0; border: 2px solid rgba(255,255,255,0.4); box-shadow: 0 4px 15px rgba(102,126,234,0.4); transition: all 0.3s ease;">
    <h4 style="margin: 0; color: white; font-size: 1.1rem; font-weight: 600; text-shadow: 0 2px 4px rgba(0,0,0,0.3);">💬 Chat Interface</h4>
</div>
""", unsafe_allow_html=True)

# --- Chat Container with compact spacing ---
chat_container = st.container()
with chat_container:
    # Render chat history with enhanced styling and icons
    if st.session_state.history:
        for i, element in enumerate(st.session_state.history):
            # Add enhanced message container with animations
            with st.container():
                st.markdown(f"""
                <div class="chat-message" style="
                    background: rgba(255,255,255,0.05);
                    border-radius: 12px;
                    padding: 0.5rem;
                    margin: 0.25rem 0;
                    border-left: 4px solid #667eea;
                    backdrop-filter: blur(5px);
                ">
                """, unsafe_allow_html=True)
                
                element.render(i)
                
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Add minimal spacing between messages
            if i < len(st.session_state.history) - 1:
                st.markdown("<div style='height: 4px;'></div>", unsafe_allow_html=True)
    else:
        # No empty state - clean minimal interface
        pass

def query_endpoint_and_render(task_type, input_messages):
    """Handle streaming response based on task type."""
    if task_type == "agent/v1/responses":
        return query_responses_endpoint_and_render(input_messages)
    elif task_type == "agent/v2/chat":
        return query_chat_agent_endpoint_and_render(input_messages)
    else:  # chat/completions
        return query_chat_completions_endpoint_and_render(input_messages)


def query_chat_completions_endpoint_and_render(input_messages):
    """Handle ChatCompletions streaming format."""
    accumulated_content = ""
    request_id = None
    
    try:
        for chunk in query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        ):
            if "choices" in chunk and chunk["choices"]:
                delta = chunk["choices"][0].get("delta", {})
                content = delta.get("content", "")
                if content:
                    accumulated_content += content
            
            if "databricks_output" in chunk:
                req_id = chunk["databricks_output"].get("databricks_request_id")
                if req_id:
                    request_id = req_id
        
        return AssistantResponse(
            messages=[{"role": "assistant", "content": accumulated_content}],
            request_id=request_id
        )
    except Exception:
        messages, request_id = query_endpoint(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        )
        return AssistantResponse(messages=messages, request_id=request_id)


def query_chat_agent_endpoint_and_render(input_messages):
    """Handle ChatAgent streaming format."""
    from mlflow.types.agent import ChatAgentChunk
    
    message_buffers = OrderedDict()
    request_id = None
    
    try:
        for raw_chunk in query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        ):
            chunk = ChatAgentChunk.model_validate(raw_chunk)
            delta = chunk.delta
            message_id = delta.id

            req_id = raw_chunk.get("databricks_output", {}).get("databricks_request_id")
            if req_id:
                request_id = req_id
            if message_id not in message_buffers:
                message_buffers[message_id] = {
                    "chunks": [],
                }
            message_buffers[message_id]["chunks"].append(chunk)
        
        messages = []
        for msg_id, msg_info in message_buffers.items():
            messages.append(reduce_chat_agent_chunks(msg_info["chunks"]))
        
        return AssistantResponse(
            messages=[message.model_dump_compat(exclude_none=True) for message in messages],
            request_id=request_id
        )
    except Exception:
        messages, request_id = query_endpoint(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        )
        return AssistantResponse(messages=messages, request_id=request_id)


def query_responses_endpoint_and_render(input_messages):
    """Handle ResponsesAgent streaming format using MLflow types."""
    from mlflow.types.responses import ResponsesAgentStreamEvent
    
    # Track all the messages that need to be rendered in order
    all_messages = []
    request_id = None

    try:
        for raw_event in query_endpoint_stream(
            endpoint_name=SERVING_ENDPOINT,
            messages=input_messages,
            return_traces=ENDPOINT_SUPPORTS_FEEDBACK
        ):
            # Extract databricks_output for request_id
            if "databricks_output" in raw_event:
                req_id = raw_event["databricks_output"].get("databricks_request_id")
                if req_id:
                    request_id = req_id
            
            # Parse using MLflow streaming event types, similar to ChatAgentChunk
            if "type" in raw_event:
                event = ResponsesAgentStreamEvent.model_validate(raw_event)
                
                if hasattr(event, 'item') and event.item:
                    item = event.item  # This is a dict, not a parsed object
                    
                    if item.get("type") == "message":
                        # Extract text content from message if present
                        content_parts = item.get("content", [])
                        for content_part in content_parts:
                            if content_part.get("type") == "output_text":
                                text = content_part.get("text", "")
                                if text:
                                    all_messages.append({
                                        "role": "assistant",
                                        "content": text
                                    })
                        
                    elif item.get("type") == "function_call":
                        # Tool call
                        call_id = item.get("call_id")
                        function_name = item.get("name")
                        arguments = item.get("arguments", "")
                        
                        # Add to messages for history
                        all_messages.append({
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [{
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": function_name,
                                    "arguments": arguments
                                }
                            }]
                        })
                        
                    elif item.get("type") == "function_call_output":
                        # Tool call output/result
                        call_id = item.get("call_id")
                        output = item.get("output", "")
                        
                        # Add to messages for history
                        all_messages.append({
                            "role": "tool",
                            "content": output,
                            "tool_call_id": call_id
                        })
            
            # Handle additional event types that contain response content
            elif raw_event.get("type") == "response.output_text.delta":
                # This contains the actual response text being streamed
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
            
            elif raw_event.get("type") == "response.output_item.done":
                # This contains the complete final message
                if "item" in raw_event:
                    item = raw_event["item"]
                    if item.get("type") == "message":
                        content_parts = item.get("content", [])
                        for content_part in content_parts:
                            if content_part.get("type") == "output_text":
                                text = content_part.get("text", "")
                                if text:
                                    # Update or add the final message
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
            
            elif raw_event.get("type") == "error":
                # Handle error events from the endpoint
                error_msg = {
                    "role": "assistant",
                    "type": "error",
                    "content": raw_event.get("message", "An error occurred"),
                    "error_code": raw_event.get("code", "unknown")
                }
                all_messages.append(error_msg)

        return AssistantResponse(messages=all_messages, request_id=request_id)
        
    except Exception as e:
        # Check if we got any error events during streaming
        error_message = None
        for msg in all_messages:
            if msg.get("type") == "error":
                error_message = msg.get("message", str(e))
                break
        
        # Only fall back to non-streaming if we don't have any messages yet
        if not all_messages:
            try:
                messages, request_id = query_endpoint(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                )
                return AssistantResponse(messages=messages, request_id=request_id)
            except Exception as fallback_error:
                error_text = error_message or str(fallback_error)
                return AssistantResponse(
                    messages=[{"role": "assistant", "content": f"**❌ Error:** {error_text}"}],
                    request_id=None
                )
        else:
            # We have messages, so the streaming was successful despite the error
            return AssistantResponse(messages=all_messages, request_id=request_id)




# --- Handle Chat Input (After Chat History Rendering) ---
prompt = st.chat_input("💬 Ask your question here...", key="chat_input")
if prompt:
    # Get the task type for this endpoint
    task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
    
    # Add user message to chat history
    user_msg = UserMessage(content=prompt)
    st.session_state.history.append(user_msg)
    
    # Show user message immediately
    with st.container():
        st.markdown(f"""
        <div class="chat-message" style="
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 0.5rem;
            margin: 0.25rem 0;
            border-left: 4px solid #667eea;
            backdrop-filter: blur(5px);
        ">
        """, unsafe_allow_html=True)
        user_msg.render(len(st.session_state.history) - 1)
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Show professional thinking animation
    thinking_placeholder = st.empty()
    with thinking_placeholder.container():
        st.markdown("""
        <div class="thinking-container" style="
            background: linear-gradient(135deg, rgba(78, 205, 196, 0.1) 0%, rgba(78, 205, 196, 0.05) 100%);
            border-radius: 15px;
            padding: 1.5rem;
            margin: 0.75rem 0;
            border: 2px solid rgba(78, 205, 196, 0.3);
            backdrop-filter: blur(10px);
            text-align: center;
            position: relative;
            overflow: hidden;
        ">
            <div style="
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 0.5rem;
                color: #4ecdc4;
                font-size: 1.2rem;
                font-weight: 700;
                text-shadow: 0 2px 4px rgba(0,0,0,0.2);
            ">
                <span style="font-size: 1.5rem;">🤖</span>
                <span>Agent is analyzing your question</span>
                <span class="thinking-dots"></span>
            </div>
            <div style="
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(90deg, transparent, rgba(78, 205, 196, 0.2), transparent);
                animation: shimmer 2s infinite;
            "></div>
        </div>
        <style>
            @keyframes shimmer {
                0% { left: -100%; }
                100% { left: 100%; }
            }
        </style>
        """, unsafe_allow_html=True)

    # Convert history to standard chat message format for the query methods
    input_messages = [msg for elem in st.session_state.history for msg in elem.to_input_messages()]
    
    # Handle the response using the appropriate handler
    assistant_response = query_endpoint_and_render(task_type, input_messages)
    
    # Clear thinking animation
    thinking_placeholder.empty()
    
    # Add assistant response to history
    if assistant_response:
        st.session_state.history.append(assistant_response)
        
        # Show assistant response immediately
        with st.container():
            st.markdown(f"""
            <div class="chat-message" style="
                background: rgba(255,255,255,0.05);
                border-radius: 12px;
                padding: 0.5rem;
                margin: 0.25rem 0;
                border-left: 4px solid #667eea;
                backdrop-filter: blur(5px);
            ">
            """, unsafe_allow_html=True)
            assistant_response.render(len(st.session_state.history) - 1)
            st.markdown("</div>", unsafe_allow_html=True)
