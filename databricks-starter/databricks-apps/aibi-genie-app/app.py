from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
import streamlit as st
import streamlit.components.v1 as components
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import os
import time
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize session state variables
if 'authentication_method' not in st.session_state:
    st.session_state.authentication_method = 'App User'
if 'current_tab' not in st.session_state:
    st.session_state.current_tab = 'source_data'

# Page configuration
st.set_page_config(
    layout="wide",
    page_title="Databricks App",
    page_icon=":chart_with_upwards_trend:",
    initial_sidebar_state="expanded"
)

# Simple, reliable CSS with subtle color touches
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    :root {
        --primary-red: #e74c3c;
        --primary-green: #27ae60;
        --primary-blue: #3498db;
        --pure-white: #ffffff;
        --text-dark: #2c3e50;
        --text-medium: #5a6c7d;
        --bg-light: #f8f9fa;
        --border-light: rgba(52, 152, 219, 0.2);
        --shadow-light: 0 2px 10px rgba(0, 0, 0, 0.1);
        --shadow-medium: 0 4px 20px rgba(0, 0, 0, 0.1);
    }
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%) !important;
        color: var(--text-dark);
    }
    
    .stApp {
        background: transparent;
    }
    
    .main .block-container {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 20px;
        margin: 20px auto;
        padding: 2rem;
        box-shadow: var(--shadow-medium);
        border: 1px solid var(--border-light);
        max-width: 1200px;
        position: relative;
    }
    
    .main .block-container::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, var(--primary-red), var(--primary-green), var(--primary-blue));
        border-radius: 20px 20px 0 0;
    }
    
    h1 {
        color: var(--text-dark) !important;
        font-weight: 700;
        font-size: 2.2rem;
        text-align: center;
        margin-bottom: 2rem;
        margin-top: 1rem;
    }
    
    h3 {
        color: var(--text-dark) !important;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    
    section[data-testid="stSidebar"] {
        background: rgba(255, 255, 255, 0.9) !important;
        border-radius: 16px;
        margin: 20px 10px;
        padding: 1.5rem;
        box-shadow: var(--shadow-light);
        border: 1px solid var(--border-light);
    }
    
    .sidebar-section {
        background: var(--pure-white);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow-light);
    }
    
    .sidebar-title {
        color: var(--text-dark) !important;
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .sidebar-label {
        color: var(--text-medium);
        font-size: 0.8rem;
        font-weight: 500;
        margin-bottom: 0.3rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .sidebar-value {
        color: var(--text-dark);
        font-size: 0.95rem;
        font-weight: 600;
        margin-bottom: 0.8rem;
        padding: 0.6rem;
        background: rgba(52, 152, 219, 0.05);
        border-radius: 8px;
        border-left: 3px solid var(--primary-blue);
    }
    
    .content-card {
        background: rgba(255, 255, 255, 0.95);
        border: 1px solid var(--border-light);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: var(--shadow-light);
        position: relative;
    }
    
    .content-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--primary-red), var(--primary-green), var(--primary-blue));
        border-radius: 16px 16px 0 0;
    }
    
    .content-card.source-data::before {
        background: var(--primary-blue);
    }
    
    .content-card.dashboard::before {
        background: var(--primary-green);
    }
    
    .content-card.genie::before {
        background: var(--primary-red);
    }
    
    .stButton > button {
        background: var(--pure-white) !important;
        border: 2px solid var(--primary-blue) !important;
        border-radius: 12px !important;
        color: var(--primary-blue) !important;
        font-weight: 600 !important;
        padding: 0.6rem 1.2rem !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        background: var(--primary-blue) !important;
        color: var(--pure-white) !important;
        transform: translateY(-2px) !important;
        box-shadow: var(--shadow-medium) !important;
    }
    
    .stDataFrame, .stTable {
        border-radius: 12px !important;
        overflow: hidden !important;
        box-shadow: var(--shadow-light) !important;
        border: 1px solid var(--border-light) !important;
    }
    
    @media (max-width: 768px) {
        .main .block-container {
            margin: 10px;
            padding: 1rem;
        }
        
        h1 {
            font-size: 1.8rem;
        }
    }
    
    </style>
""", unsafe_allow_html=True)

# Helper Functions
def _get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
        access_token=headers.get("X-Forwarded-Access-Token")
    )

def credential_provider():
    # In Databricks Apps, use OAuth service principal (automatically configured)
    # If running locally, fall back to PAT token
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    if client_id and client_secret:
        # Running in Databricks Apps - use OAuth
        config = Config(
            host=f'https://{os.getenv("DATABRICKS_HOST")}',
            client_id=client_id,
            client_secret=client_secret)
        return oauth_service_principal(config)
    else:
        # Running locally - use PAT token
        return lambda: os.getenv("DATABRICKS_TOKEN")

def execute_sql_query(query_text):
    try:
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            credentials_provider=credential_provider
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query_text)
                result = cursor.fetchall_arrow().to_pandas()
            return result
    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")
        return pd.DataFrame()

def do_api_call(endpoint, payload, headers, method):
    print("API URL: ", endpoint)
    try:
        if method == "GET":
            response = requests.get(endpoint, headers=headers)
        elif method == "POST":
            response = requests.post(endpoint, json=payload, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API call failed with status code {response.status_code}")
            if response.text:
                st.error(f"Response: {response.text}")
            return None
    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        return None

def validate_user_input(prompt: str) -> bool:
    """Validate user input for the Genie Space prompt."""
    if not prompt or len(prompt.strip()) == 0:
        st.error("Please enter a valid prompt")
        return False
    if len(prompt) > 1000:
        st.error("Prompt is too long. Please keep it under 1000 characters")
        return False
    return True

@st.cache_data(ttl=30)
def get_data(method):
    os.write(1, f"Fetching data from Databricks using {method}".encode())
    df = execute_sql_query("SELECT * FROM kaustavpaul_demo.SP500.gold_sp500_analytics;")
    return pd.DataFrame(df)

def get_uc_df():
    """Get data using the current authentication method."""
    return get_data(st.session_state.authentication_method)

# Get user information
user_info = _get_user_info()
print("user_info: ", user_info, sep="")

# Sidebar
with st.sidebar:
    st.markdown(
        "<div class='sidebar-section'>"
        "<div class='sidebar-title'>👤 User Information</div>"
        f"<div class='sidebar-label'>Email:</div><div class='sidebar-value'>{user_info.get('user_email')}</div>"
        f"<div class='sidebar-label'>User ID:</div><div class='sidebar-value'>{user_info.get('user_id').split('@')[0]}</div>"
        "</div>",
        unsafe_allow_html=True
    )
    st.markdown(
        "<div class='sidebar-section'>"
        "<div class='sidebar-title'>🔒 Authentication</div>"
        f"<div class='sidebar-label'>Method:</div><div class='sidebar-value'>{st.session_state.authentication_method}</div>"
        "</div>",
        unsafe_allow_html=True
    )

# Main content
st.title("📊 S&P500 Analytics App")
st.markdown("---")

# Clean Navigation Buttons
tab_labels = ["📈 Source Data", "🤖 AI/BI Dashboard", "✨ Genie Space"]
tab_keys = ["source_data", "dashboard", "genie"]
nav_cols = st.columns(len(tab_keys))

for i, (tab_key, tab_label) in enumerate(zip(tab_keys, tab_labels)):
    is_active = st.session_state.current_tab == tab_key
    
    with nav_cols[i]:
        if st.button(tab_label, key=f"nav_{tab_key}", use_container_width=True):
            st.session_state.current_tab = tab_key
            st.rerun()
        
        # Apply clean styling based on active state
        st.markdown(f"""
        <style>
        div[data-testid='column']:nth-child({i+1}) button {{
            border-radius: 12px !important;
            padding: 0.8rem 1.2rem !important;
            margin: 0.2rem !important;
            font-size: 0.95rem !important;
            min-height: 3rem !important;
            transition: all 0.3s ease !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        
        # Apply simple, reliable styling
        if is_active:
            st.markdown(f"""
            <style>
            div[data-testid='column']:nth-child({i+1}) button {{
                background: var(--primary-blue) !important;
                color: var(--pure-white) !important;
                border: 2px solid var(--primary-blue) !important;
                font-weight: 700 !important;
            }}
            </style>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <style>
            div[data-testid='column']:nth-child({i+1}) button {{
                background: rgba(255, 255, 255, 0.8) !important;
                color: var(--text-dark) !important;
                border: 1px solid var(--border-light) !important;
                font-weight: 600 !important;
            }}
            
            div[data-testid='column']:nth-child({i+1}) button:hover {{
                background: rgba(255, 255, 255, 0.95) !important;
                border-color: var(--primary-blue) !important;
                color: var(--primary-blue) !important;
            }}
            </style>
            """, unsafe_allow_html=True)

# Tab content based on current_tab
current_tab = st.session_state.current_tab

# Tab 1: Source Data
if current_tab == 'source_data':
    st.markdown("""
    <div class="content-card source-data">
        <h3>📊 Source Data Details & Sample (First 100 Records)</h3>
    """, unsafe_allow_html=True)
    
    # Loading animation for data fetching
    with st.spinner('🔄 Loading data from Databricks...'):
        data = get_uc_df()
    
    if not data.empty:
        st.markdown("**Source Data:**  ")
        st.markdown("- **Catalog:** kaustavpaul_demo  ")
        st.markdown("- **Schema:** SP500  ")
        st.markdown("- **Table:** gold_sp500_analytics  ")
        st.markdown(f"**Total records in table:** {len(data):,}")
        
        # Animated dataframe display
        st.markdown("""
        <div class="fade-in">
        """, unsafe_allow_html=True)
        st.dataframe(data.head(100), height=400, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.error("❌ Failed to load data. Please check your connection.")
    
    st.markdown("</div>", unsafe_allow_html=True)

# Tab 2: AI/BI Dashboard
elif current_tab == 'dashboard':
    st.markdown("""
    <div class="content-card dashboard">
        <h3>🤖 AI/BI Dashboard</h3>
    """, unsafe_allow_html=True)
    
    AI_BI_Dashboard_URL = 'https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f02add28541653aaab274f5d322d1b?o=1444828305810485'
    
    # Loading animation for iframe
    with st.spinner('🔄 Loading AI/BI Dashboard...'):
        components.iframe(AI_BI_Dashboard_URL, height=1050, scrolling=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

# Tab 3: Genie Space
elif current_tab == 'genie':
    st.markdown("""
    <div class="content-card genie">
        <h3>✨ Let's explore the S&P500 data through genie space</h3>
    """, unsafe_allow_html=True)
    
    # Brief description of the data
    st.markdown("""
    The S&P 500 dataset contains historical and analytical data about the S&P 500 index and its constituent companies. You can explore trends, sector breakdowns, market capitalizations, and price movements over time. Use Genie Space to ask questions and gain insights from the data.
    """)
    
    # Reset button with modern styling
    if st.button("🔄 Reset Page", type="secondary", use_container_width=True):
        st.session_state['genie_suggested'] = ""
        st.session_state['genie_input'] = ""
        st.session_state['genie_response'] = None
        st.rerun()
    
    # Suggested questions with modern styling
    st.markdown("**✨ Suggested questions:**")
    colA, colB = st.columns([1, 3])
    with colA:
        if st.button("✨ Explain the data set", use_container_width=True):
            st.session_state['genie_suggested'] = "Explain the data set"
    with colB:
        if st.button("❓ What is the average market capitalization of companies in the SP500?", use_container_width=True):
            st.session_state['genie_suggested'] = "What is the average market capitalization of companies in the SP500?"
        if st.button("❓ What are the most common sectors represented in the SP500 companies?", use_container_width=True):
            st.session_state['genie_suggested'] = "What are the most common sectors represented in the SP500 companies?"
        if st.button("❓ What is the monthly average closing price of SP500 over the last year?", use_container_width=True):
            st.session_state['genie_suggested'] = "What is the monthly average closing price of SP500 over the last year?"
    
    # Construct Genie Space URL using environment variables
    databricks_host = os.getenv("DATABRICKS_HOST")
    space_id = "01f050501b7912148a8ee89a422369d6"
    genie_space_url = f"https://{databricks_host}/api/2.0/genie/spaces/{space_id}"
    print("genie_space_url: ", genie_space_url, sep="")
    
    # Use either the suggested question or user input
    default_prompt = st.session_state.get('genie_suggested', "")
    user_input = st.text_input("Enter your prompt here:", value=default_prompt, key="genie_input", placeholder="e.g., What is the average closing price for the last 30 days?")
    st.session_state['genie_suggested'] = ""  # Reset after use

    if user_input:
        # For Genie API, use dedicated PAT token (has required scopes)
        genie_token = os.getenv('DATABRICKS_TOKEN_FOR_GENIE')
        
        if not genie_token:
            st.error("❌ DATABRICKS_TOKEN_FOR_GENIE environment variable is not set. Please check your app configuration.")
            st.stop()
            
        auth_token = genie_token
        
        payload = {'content': user_input}
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }

        submit_message_res = do_api_call(f'{genie_space_url}/start-conversation', payload, headers, "POST")
        print("submit message res ", submit_message_res)

        if submit_message_res:
            conversation_id = submit_message_res['conversation_id']
            message_id = submit_message_res['message_id']
            print("conversation_id ", conversation_id)
            print("message_id ", message_id)

            with st.spinner('🔄 Processing your request...'):
                for _ in range(1000):
                    message_status = do_api_call(f'{genie_space_url}/conversations/{conversation_id}/messages/{message_id}', None, headers, "GET")
                    print("message status res ", message_status)
                    if message_status is not None:
                        curr_status = message_status["status"]
                        print(f"Status: ", curr_status)
                        if curr_status == "COMPLETED":
                            break
                    time.sleep(1)
            
                message_result = do_api_call(f'{genie_space_url}/conversations/{conversation_id}/messages/{message_id}/query-result', None, headers, "GET")
                print("message result: ", message_result)

                if not message_result:
                    if message_status and "attachments" in message_status:
                        for attachment in message_status["attachments"]:
                            if "text" in attachment and "content" in attachment["text"]:
                                st.write(attachment["text"]["content"])
                            else:
                                st.write("Genie Space API Response:", attachment)
                else:
                    try:
                        data = message_result['statement_response']['result']
                        meta = message_result['statement_response']['manifest']
                        rows = [[c['str'] for c in r['values']] for r in data['data_typed_array']]
                        columns = [c['name'] for c in meta['schema']['columns']]
                        res_df = pd.DataFrame(data=rows, columns=columns)
                        
                        # Animated dataframe display
                        st.markdown("""
                        <div class="fade-in">
                        """, unsafe_allow_html=True)
                        st.dataframe(data=res_df, height=400, use_container_width=True)
                        st.markdown("</div>", unsafe_allow_html=True)
                        
                        # Collapsible API response section
                        with st.expander("🔧 View Complete API Response (Technical Details)", expanded=False):
                            st.write("Complete Genie Space API Response:", message_result)
                    except Exception as e:
                        st.error(f"Error processing response: {str(e)}")
                        if message_status and "attachments" in message_status:
                            for attachment in message_status["attachments"]:
                                if "text" in attachment and "content" in attachment["text"]:
                                    st.write(attachment["text"]["content"])
    
    st.markdown("</div>", unsafe_allow_html=True)
