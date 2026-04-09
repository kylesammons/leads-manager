import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import time
from datetime import date, timedelta

# Set Streamlit page config
st.set_page_config(page_title="Leads Manager", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# BigQuery configuration
PROJECT_ID = "trimark-tdp"

@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    try:
        credentials = None

        # Method 1: Try Streamlit secrets (for deployment)
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"]
                )
        except Exception:
            pass

        # Method 2: Try environment variable (recommended for local)
        if not credentials:
            try:
                credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if credentials_path and os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
            except Exception:
                pass

        # Method 3: Try hardcoded path (fallback for local development)
        if not credentials:
            try:
                hardcoded_path = '/Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
                if os.path.exists(hardcoded_path):
                    credentials = service_account.Credentials.from_service_account_file(hardcoded_path)
            except Exception:
                pass

        if not credentials:
            raise Exception("No valid credentials found. Please check your setup.")

        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        return client

    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        return None


@st.cache_data(ttl=300)
def load_client_credentials():
    """Load client credentials from CSV file"""
    try:
        csv_path = "The Reef - Clients.csv"

        if not os.path.exists(csv_path):
            st.error(f"Client credentials file not found: {csv_path}")
            st.info("Please ensure 'The Reef - Clients.csv' is in the same directory as this app.")
            return pd.DataFrame()

        df = pd.read_csv(csv_path)

        if 'Client_Name' not in df.columns or 'Client_ID' not in df.columns:
            st.error("CSV file must contain 'Client_Name' and 'Client_ID' columns")
            return pd.DataFrame()

        return df

    except Exception as e:
        st.error(f"Error loading client credentials: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def verify_login(username, password):
    """Verify login credentials against CSV file"""
    try:
        clients_df = load_client_credentials()

        if clients_df.empty:
            return None, None

        username_normalized = username.lower().strip().replace(" ", "")
        clients_df['normalized_name'] = clients_df['Client_Name'].str.lower().str.strip().str.replace(" ", "")
        clients_df['Client_ID'] = clients_df['Client_ID'].astype(str)

        match = clients_df[
            (clients_df['normalized_name'] == username_normalized) &
            (clients_df['Client_ID'] == password)
        ]

        if len(match) > 0:
            return match.iloc[0]['Client_Name'], match.iloc[0]['Client_ID']
        else:
            return None, None

    except Exception as e:
        st.error(f"Error verifying login: {str(e)}")
        return None, None


def load_form_leads(client_id, start_date, end_date):
    """Load form leads from BigQuery using the Window World form leads query"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()

    try:
        try:
            client_id_int = int(client_id)
            client_id_filter = f"ref.client_id = {client_id_int}"
        except (ValueError, TypeError):
            client_id_filter = f"ref.client_id = '{client_id}'"

        query = f"""
        SELECT
            clientref.client_name,
            tb.* EXCEPT(Body, source),
            CASE
                WHEN source = 'Microsoft' THEN 'Microsoft Ads'
                WHEN source = 'Google'    THEN 'Google Ads'
                WHEN source = 'Facebook'  THEN 'Facebook Ads'
                ELSE source
            END AS Source
        FROM `trimark-tdp.windowworld.form_leads` AS tb
        JOIN `reference.formleads_ref`  AS ref       ON ref.From = tb.From
        JOIN `reference.client_ref`     AS clientref ON clientref.client_id = ref.client_id
        WHERE
            {client_id_filter}
            AND tb.date BETWEEN '{start_date}' AND '{end_date}'
            AND tb.Body NOT LIKE '%test%'
            AND tb.Body NOT LIKE '%TEST%'
            AND tb.Body NOT LIKE '%Test%'
            AND tb.Email NOT LIKE '%jasmine.atari@gmail.com%'
            AND tb.Email NOT LIKE '%charvard1@shopcobe.com%'
        ORDER BY tb.date DESC
        """

        df = client.query(query).to_dataframe()
        return df

    except Exception as e:
        st.error(f"Error loading form leads: {str(e)}")
        return pd.DataFrame()


def load_call_leads(client_id, start_date, end_date):
    """Load call leads from BigQuery using the Marchex query"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()

    try:
        try:
            client_id_int = int(client_id)
            client_id_filter = f"ref.Client_ID = {client_id_int}"
        except (ValueError, TypeError):
            client_id_filter = f"ref.Client_ID = '{client_id}'"

        query = f"""
        SELECT
            ref.Client_Name,
            mx2.id            AS lead_id,
            CAST(mx2.start_time AS DATE) AS date,
            mx2.name,
            mx2.caller_number,
            mx2.call_duration,
            mx2.address,
            mx2.city,
            mx2.state,
            mx2.zip_code
        FROM `trimark-tdp.platform.marchex_campaign_*` AS mx2
        JOIN `reference.marchex_ref` AS ref
            ON CASE
                WHEN mx2.group_owner_name = 'Window World Baton Rouge LA/Tampa FL'
                    THEN mx2.group_name = ref.c_name
                ELSE ref.Account_Name = mx2.group_owner_name
               END
        JOIN `reference.client_ref` AS clientref
            ON clientref.client_id = ref.Client_ID
        WHERE
            {client_id_filter}
            AND DATE(mx2.start_time) BETWEEN '{start_date}' AND '{end_date}'
            AND DATE(mx2.start_time) < CURRENT_DATE()
        ORDER BY date DESC
        """

        df = client.query(query).to_dataframe()
        return df

    except Exception as e:
        st.error(f"Error loading call leads: {str(e)}")
        return pd.DataFrame()


def display_scorecards(form_df, call_df):
    """Display simplified scorecard metrics"""
    total_leads = len(form_df) + len(call_df)
    form_leads  = len(form_df)
    call_leads  = len(call_df)

    st.markdown("""
    <style>
    .scorecard {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        background: white;
        color: #333;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .scorecard-value {
        font-size: 32px;
        font-weight: bold;
        margin: 10px 0;
        color: #1f77b4;
    }
    .scorecard-label {
        font-size: 14px;
        color: #666;
        font-weight: 500;
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Total Leads</div>
            <div class="scorecard-value">{total_leads}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Form Leads</div>
            <div class="scorecard-value">{form_leads}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Call Leads</div>
            <div class="scorecard-value">{call_leads}</div>
        </div>
        """, unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "client_name" not in st.session_state:
    st.session_state.client_name = None
if "client_id" not in st.session_state:
    st.session_state.client_id = None

# ── Login page ────────────────────────────────────────────────────────────────
if not st.session_state.authenticated:
    st.title("Leads Manager")
    st.markdown("---")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col2:
        st.subheader("Login")

        username = st.text_input("Username", placeholder="e.g., windowworldof...")
        password = st.text_input("Password", type="password", placeholder="Enter your Client Pin")

        if st.button("Login", type="primary", use_container_width=True):
            if username and password:
                client_name, client_id = verify_login(username, password)

                if client_name and client_id:
                    st.session_state.authenticated = True
                    st.session_state.client_name   = client_name
                    st.session_state.client_id     = client_id
                    st.success(f"Welcome, {client_name}!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Invalid username or password")
            else:
                st.warning("Please enter both username and password")

    st.stop()

# ── Main application ──────────────────────────────────────────────────────────
st.title(f"{st.session_state.client_name} Leads Manager")

with st.sidebar:
    st.image("Waves-Logo_Color.svg", width=200)
    st.markdown("<br>", unsafe_allow_html=True)

    st.subheader("📅 Date Range")

    with st.expander("Select Date Range", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=date.today().replace(day=1),
                help="Select start date"
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=date.today(),
                help="Select end date"
            )

    st.markdown("---")

    if st.button("🚪 Logout", use_container_width=True):
        st.session_state.authenticated = False
        st.session_state.client_name   = None
        st.session_state.client_id     = None
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading leads data..."):
    form_leads_df = load_form_leads(st.session_state.client_id, start_date, end_date)
    call_leads_df = load_call_leads(st.session_state.client_id, start_date, end_date)

# ── Scorecards ────────────────────────────────────────────────────────────────
display_scorecards(form_leads_df, call_leads_df)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["Form Leads", "Call Leads"])

with tab1:
    st.header("Form Leads")

    if not form_leads_df.empty:
        st.write(f"Total Form Leads: `{len(form_leads_df)}`")
        st.dataframe(
            form_leads_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No form leads data available for the selected date range.")

with tab2:
    st.header("Call Leads")

    if not call_leads_df.empty:
        st.write(f"Total Call Leads: `{len(call_leads_df)}`")
        st.dataframe(
            call_leads_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No call leads data available for the selected date range.")
