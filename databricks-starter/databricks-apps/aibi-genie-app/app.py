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

# Configuration constants
GENIE_TIMEOUT_SECONDS = 300  # 5 minutes default timeout for Genie queries
GENIE_POLL_INITIAL_INTERVAL = 1  # Start with 1 second
GENIE_POLL_MAX_INTERVAL = 5  # Max 5 seconds between polls

# Error message mapping for user-friendly error display
ERROR_MESSAGES = {
    400: {
        'title': '❌ Invalid Request',
        'message': 'The query could not be processed. Please check your input.',
        'action': 'Try rephrasing your question or use one of the suggested queries.'
    },
    401: {
        'title': '🔒 Authentication Failed',
        'message': 'Unable to authenticate with Databricks.',
        'action': 'Check that your authentication tokens are valid and have the required permissions.'
    },
    403: {
        'title': '🚫 Access Denied',
        'message': 'You don\'t have permission to access this resource.',
        'action': 'Contact your workspace administrator to grant access to the Genie Space or SQL Warehouse.'
    },
    404: {
        'title': '🔍 Resource Not Found',
        'message': 'The requested resource could not be found.',
        'action': 'Verify that the Genie Space ID and endpoints are correctly configured.'
    },
    429: {
        'title': '⏸️ Rate Limited',
        'message': 'Too many requests. Please wait a moment.',
        'action': 'Wait a few seconds and try again.'
    },
    500: {
        'title': '⚠️ Server Error',
        'message': 'The Databricks service encountered an error.',
        'action': 'Try again in a moment. If the issue persists, check the service status.'
    },
    503: {
        'title': '🔧 Service Unavailable',
        'message': 'The service is temporarily unavailable.',
        'action': 'The service may be under maintenance. Please try again later.'
    }
}

# Initialize session state variables
if 'authentication_method' not in st.session_state:
    st.session_state.authentication_method = 'App User'
if 'current_tab' not in st.session_state:
    st.session_state.current_tab = 'source_data'
# Pagination state
if 'page_number' not in st.session_state:
    st.session_state.page_number = 1
if 'page_size' not in st.session_state:
    st.session_state.page_size = 100

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



def display_error(status_code: int, response_text: Optional[str] = None, context: Optional[str] = None):
    """
    Display user-friendly error messages with actionable guidance.
    
    Args:
        status_code: HTTP status code
        response_text: Raw response text from API
        context: Additional context about the error
    """
    error_info = ERROR_MESSAGES.get(status_code, {
        'title': '❌ Unexpected Error',
        'message': f'An unexpected error occurred (Status: {status_code}).',
        'action': 'Please try again or contact support if the issue persists.'
    })
    
    st.error(f"**{error_info['title']}**")
    st.warning(error_info['message'])
    st.info(f"**💡 Suggested Action:** {error_info['action']}")
    
    # Technical details in expander
    with st.expander("🔧 Technical Details", expanded=False):
        st.code(f"Status Code: {status_code}")
        if context:
            st.code(f"Context: {context}")
        if response_text:
            st.code(f"Response: {response_text}")


def poll_genie_message_with_timeout(
    genie_space_url: str,
    conversation_id: str,
    message_id: str,
    headers: Dict[str, str],
    timeout_seconds: int = GENIE_TIMEOUT_SECONDS
) -> Optional[Dict[str, Any]]:
    """
    Poll Genie message status with configurable timeout and progress updates.
    
    Args:
        genie_space_url: Base URL for Genie Space API
        conversation_id: Conversation ID
        message_id: Message ID to poll
        headers: HTTP headers for authentication
        timeout_seconds: Maximum wait time in seconds
    
    Returns:
        Message status dictionary or None if timeout/error
    
    Raises:
        TimeoutError: If polling exceeds timeout
    """
    start_time = time.time()
    poll_interval = GENIE_POLL_INITIAL_INTERVAL
    progress_placeholder = st.empty()
    
    try:
        while time.time() - start_time < timeout_seconds:
            elapsed = int(time.time() - start_time)
            progress_placeholder.info(f"⏱️ Processing your request... ({elapsed}s elapsed)")
            
            endpoint = f'{genie_space_url}/conversations/{conversation_id}/messages/{message_id}'
            message_status = do_api_call(endpoint, None, headers, "GET")
            
            if message_status is not None:
                curr_status = message_status.get("status")
                logger.info(f"Genie message status: {curr_status}")
                
                if curr_status == "COMPLETED":
                    progress_placeholder.empty()
                    return message_status
                elif curr_status == "FAILED":
                    progress_placeholder.empty()
                    st.error("❌ The query failed to process. Please try a different question.")
                    return message_status
            
            # Exponential backoff with max interval
            time.sleep(min(poll_interval, GENIE_POLL_MAX_INTERVAL))
            poll_interval *= 1.2
        
        # Timeout reached
        progress_placeholder.empty()
        st.error(f"⏱️ **Request Timeout**")
        st.warning(f"The request took longer than {timeout_seconds} seconds to process.")
        st.info("**💡 Suggested Actions:**\n- Try a simpler query\n- Break complex questions into smaller parts\n- Check if the data source is accessible")
        raise TimeoutError(f"Genie query timed out after {timeout_seconds} seconds")
    
    except TimeoutError:
        raise
    except Exception as e:
        progress_placeholder.empty()
        logger.error(f"Error polling Genie message: {str(e)}")
        st.error(f"An error occurred while processing your request: {str(e)}")
        return None


def _get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
        access_token=headers.get("X-Forwarded-Access-Token")
    )

def credential_provider():
    """
    Credential provider for SQL connections.
    In Databricks Apps, uses OAuth service principal (automatically configured).
    For local development, falls back to PAT token.
    """
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    if client_id and client_secret:
        # Running in Databricks Apps - use OAuth
        logger.info("Using OAuth service principal authentication")
        config = Config(
            host=f'https://{os.getenv("DATABRICKS_HOST")}',
            client_id=client_id,
            client_secret=client_secret)
        return oauth_service_principal(config)
    else:
        # Running locally - use PAT token
        logger.info("Using PAT token authentication")
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

def do_api_call(endpoint: str, payload: Optional[Dict], headers: Dict[str, str], method: str) -> Optional[Dict]:
    """
    Make API calls with enhanced error handling and user-friendly messages.
    
    Args:
        endpoint: API endpoint URL
        payload: Request payload (for POST requests)
        headers: HTTP headers
        method: HTTP method (GET or POST)
    
    Returns:
        Response JSON or None if error
    """
    logger.info(f"API {method} call to: {endpoint}")
    
    try:
        if method == "GET":
            response = requests.get(endpoint, headers=headers, timeout=30)
        elif method == "POST":
            response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
        else:
            st.error(f"Unsupported HTTP method: {method}")
            return None
        
        if response.status_code == 200:
            logger.info(f"API call successful: {endpoint}")
            return response.json()
        else:
            logger.warning(f"API call failed with status {response.status_code}: {endpoint}")
            display_error(
                status_code=response.status_code,
                response_text=response.text[:500] if response.text else None,
                context=f"Endpoint: {method} {endpoint}"
            )
            return None
    
    except requests.exceptions.Timeout:
        st.error("⏱️ **Request Timeout**")
        st.warning("The request took too long to complete.")
        st.info("**💡 Suggested Action:** Check your network connection and try again.")
        logger.error(f"Timeout error for endpoint: {endpoint}")
        return None
    except requests.exceptions.ConnectionError:
        st.error("🔌 **Connection Error**")
        st.warning("Unable to connect to the Databricks service.")
        st.info("**💡 Suggested Action:** Check your network connection and service availability.")
        logger.error(f"Connection error for endpoint: {endpoint}")
        return None
    except Exception as e:
        st.error(f"❌ **Unexpected Error**")
        st.warning(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Unexpected error in API call: {str(e)}", exc_info=True)
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
    """Legacy function - fetches all data (kept for backward compatibility)."""
    os.write(1, f"Fetching data from Databricks using {method}".encode())
    df = execute_sql_query("SELECT * FROM kaustavpaul_demo.SP500.gold_sp500_analytics;")
    return pd.DataFrame(df)


@st.cache_data(ttl=30)
def get_data_paginated(method: str, limit: int, offset: int) -> pd.DataFrame:
    """
    Fetch paginated data from Databricks.
    
    Args:
        method: Authentication method (for cache key)
        limit: Number of records to fetch
        offset: Number of records to skip
    
    Returns:
        DataFrame with paginated results
    """
    logger.info(f"Fetching paginated data: limit={limit}, offset={offset}")
    query = f"""
        SELECT * FROM kaustavpaul_demo.SP500.gold_sp500_analytics 
        ORDER BY Date DESC
        LIMIT {limit} OFFSET {offset}
    """
    df = execute_sql_query(query)
    return pd.DataFrame(df)


@st.cache_data(ttl=300)
def get_total_count() -> int:
    """
    Get total record count from the table.
    Cached for 5 minutes to reduce database load.
    
    Returns:
        Total number of records
    """
    logger.info("Fetching total record count")
    query = "SELECT COUNT(*) as total FROM kaustavpaul_demo.SP500.gold_sp500_analytics"
    result = execute_sql_query(query)
    if not result.empty:
        total = int(result.iloc[0]['total'])
        logger.info(f"Total records: {total}")
        return total
    return 0


def get_uc_df():
    """Get data using the current authentication method (legacy - non-paginated)."""
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

# Tab 1: Source Data with Pagination
if current_tab == 'source_data':
    st.markdown("""
    <div class="content-card source-data">
        <h3>📊 Source Data Details & Records</h3>
    """, unsafe_allow_html=True)
    
    # Data source information
    st.markdown("**Source Data:**  ")
    st.markdown("- **Catalog:** kaustavpaul_demo  ")
    st.markdown("- **Schema:** SP500  ")
    st.markdown("- **Table:** gold_sp500_analytics  ")
    
    # Get total count
    with st.spinner('🔄 Loading data information...'):
        total_records = get_total_count()
    
    if total_records > 0:
        st.markdown(f"**Total records in table:** {total_records:,}")
        st.markdown("---")
        
        # Pagination controls - Page size selector
        col1, col2, col3 = st.columns([2, 3, 2])
        with col1:
            page_size_options = [50, 100, 200, 500]
            current_page_size = st.session_state.page_size
            
            new_page_size = st.selectbox(
                "📄 Records per page:",
                page_size_options,
                index=page_size_options.index(current_page_size) if current_page_size in page_size_options else 1,
                key="page_size_selector"
            )
            
            if new_page_size != st.session_state.page_size:
                st.session_state.page_size = new_page_size
                st.session_state.page_number = 1  # Reset to first page
                st.rerun()
        
        # Calculate pagination parameters
        page_size = st.session_state.page_size
        page_number = st.session_state.page_number
        offset = (page_number - 1) * page_size
        total_pages = (total_records + page_size - 1) // page_size
        
        # Ensure page number is within bounds
        if page_number > total_pages:
            st.session_state.page_number = total_pages
            st.rerun()
        
        # Fetch paginated data
        with st.spinner(f'🔄 Loading page {page_number} of {total_pages}...'):
            data = get_data_paginated(st.session_state.authentication_method, page_size, offset)
        
        if not data.empty:
            # Pagination navigation buttons
            col_prev, col_info, col_next = st.columns([1, 3, 1])
            
            with col_prev:
                if st.button("⬅️ Previous", disabled=(page_number <= 1), use_container_width=True):
                    st.session_state.page_number -= 1
                    st.rerun()
            
            with col_info:
                start_record = offset + 1
                end_record = min(offset + page_size, total_records)
                st.markdown(
                    f"<div style='text-align: center; padding: 0.5rem;'>"
                    f"<strong>Showing {start_record:,} - {end_record:,} of {total_records:,} records</strong><br>"
                    f"<span style='color: var(--text-medium);'>Page {page_number} of {total_pages}</span>"
                    f"</div>",
                    unsafe_allow_html=True
                )
            
            with col_next:
                if st.button("Next ➡️", disabled=(page_number >= total_pages), use_container_width=True):
                    st.session_state.page_number += 1
                    st.rerun()
            
            # Display data
            st.markdown("""
            <div class="fade-in">
            """, unsafe_allow_html=True)
            st.dataframe(data, height=400, use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Jump to page feature
            with st.expander("⚡ Jump to Specific Page"):
                col_jump1, col_jump2 = st.columns([3, 1])
                with col_jump1:
                    jump_page = st.number_input(
                        "Page number:",
                        min_value=1,
                        max_value=total_pages,
                        value=page_number,
                        step=1,
                        key="jump_page_input"
                    )
                with col_jump2:
                    if st.button("Go", key="jump_page_button", use_container_width=True):
                        if jump_page != page_number:
                            st.session_state.page_number = jump_page
                            st.rerun()
            
            # Display current page info
            st.info(f"💡 **Tip:** Use the page size selector to view more records at once, or jump to a specific page using the controls above.")
        else:
            st.warning("⚠️ No data found for the current page.")
    else:
        st.error("❌ Failed to load data. Please check your connection and table access permissions.")
    
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

        # Submit the query
        submit_message_res = do_api_call(f'{genie_space_url}/start-conversation', payload, headers, "POST")
        logger.info(f"Genie conversation started: {submit_message_res}")

        if submit_message_res:
            conversation_id = submit_message_res.get('conversation_id')
            message_id = submit_message_res.get('message_id')
            
            if not conversation_id or not message_id:
                st.error("❌ Failed to start conversation. Invalid response from Genie API.")
                logger.error(f"Invalid response: {submit_message_res}")
                st.stop()
            
            logger.info(f"Conversation ID: {conversation_id}, Message ID: {message_id}")

            try:
                # Poll for results with timeout
                with st.spinner('🔄 Processing your request...'):
                    message_status = poll_genie_message_with_timeout(
                        genie_space_url, 
                        conversation_id, 
                        message_id, 
                        headers,
                        timeout_seconds=GENIE_TIMEOUT_SECONDS
                    )
                
                if message_status and message_status.get("status") == "COMPLETED":
                    # Fetch query results
                    message_result = do_api_call(
                        f'{genie_space_url}/conversations/{conversation_id}/messages/{message_id}/query-result', 
                        None, 
                        headers, 
                        "GET"
                    )
                    
                    logger.info(f"Query result received: {bool(message_result)}")

                    if not message_result:
                        # Try to show attachments if no query result
                        if message_status and "attachments" in message_status:
                            st.info("📄 **Response:**")
                            for attachment in message_status["attachments"]:
                                if "text" in attachment and "content" in attachment["text"]:
                                    st.markdown(attachment["text"]["content"])
                                else:
                                    st.write("Genie Space API Response:", attachment)
                        else:
                            st.warning("⚠️ No results returned from Genie Space.")
                    else:
                        try:
                            # Parse and display tabular results
                            data = message_result.get('statement_response', {}).get('result', {})
                            meta = message_result.get('statement_response', {}).get('manifest', {})
                            
                            if data and meta:
                                rows = [[c.get('str', '') for c in r.get('values', [])] for r in data.get('data_typed_array', [])]
                                columns = [c.get('name', f'Column_{i}') for i, c in enumerate(meta.get('schema', {}).get('columns', []))]
                                
                                if rows and columns:
                                    res_df = pd.DataFrame(data=rows, columns=columns)
                                    
                                    st.success("✅ Query completed successfully!")
                                    
                                    # Animated dataframe display
                                    st.markdown("""
                                    <div class="fade-in">
                                    """, unsafe_allow_html=True)
                                    st.dataframe(data=res_df, height=400, use_container_width=True)
                                    st.markdown("</div>", unsafe_allow_html=True)
                                    
                                    # Collapsible API response section
                                    with st.expander("🔧 View Complete API Response (Technical Details)", expanded=False):
                                        st.json(message_result)
                                else:
                                    st.warning("⚠️ Query completed but returned no data.")
                            else:
                                st.info("📄 Query completed. Check response details below.")
                                with st.expander("🔧 View API Response", expanded=True):
                                    st.json(message_result)
                        
                        except KeyError as e:
                            st.warning(f"⚠️ Unexpected response format: {str(e)}")
                            logger.error(f"Error parsing result: {str(e)}", exc_info=True)
                            
                            # Show attachments as fallback
                            if message_status and "attachments" in message_status:
                                st.info("📄 **Response Content:**")
                                for attachment in message_status["attachments"]:
                                    if "text" in attachment and "content" in attachment["text"]:
                                        st.markdown(attachment["text"]["content"])
                        
                        except Exception as e:
                            st.error(f"❌ Error processing response: {str(e)}")
                            logger.error(f"Error processing Genie response: {str(e)}", exc_info=True)
                            
                            # Show raw response in expander
                            with st.expander("🔧 Raw API Response", expanded=False):
                                st.write(message_result)
                
                elif message_status and message_status.get("status") == "FAILED":
                    # Query failed - try to show error details
                    st.error("❌ Query execution failed.")
                    if "attachments" in message_status:
                        for attachment in message_status["attachments"]:
                            if "text" in attachment and "content" in attachment["text"]:
                                st.warning(attachment["text"]["content"])
                
            except TimeoutError as e:
                logger.warning(f"Genie query timeout: {str(e)}")
                # Error already displayed by poll_genie_message_with_timeout
                pass
            
            except Exception as e:
                st.error(f"❌ An unexpected error occurred: {str(e)}")
                logger.error(f"Unexpected error in Genie processing: {str(e)}", exc_info=True)
    
    st.markdown("</div>", unsafe_allow_html=True)
