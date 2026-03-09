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
        except Exception as e:
            pass
        
        # Method 2: Try environment variable (recommended for local)
        if not credentials:
            try:
                credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if credentials_path and os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
            except Exception as e:
                pass
        
        # Method 3: Try hardcoded path (fallback for local development)
        if not credentials:
            try:
                hardcoded_path = '/Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
                if os.path.exists(hardcoded_path):
                    credentials = service_account.Credentials.from_service_account_file(hardcoded_path)
            except Exception as e:
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
        # Try to load from the same directory as the script
        csv_path = "The Reef - Clients.csv"
        
        if not os.path.exists(csv_path):
            st.error(f"Client credentials file not found: {csv_path}")
            st.info("Please ensure 'The Reef - Clients.csv' is in the same directory as this app.")
            return pd.DataFrame()
        
        df = pd.read_csv(csv_path)
        
        # Ensure required columns exist
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
        # Load client credentials from CSV
        clients_df = load_client_credentials()
        
        if clients_df.empty:
            return None, None
        
        # Normalize username (lowercase and trim spaces)
        username_normalized = username.lower().strip().replace(" ", "")
        
        # Normalize Client_Name in dataframe for comparison
        clients_df['normalized_name'] = clients_df['Client_Name'].str.lower().str.strip().str.replace(" ", "")
        
        # Convert Client_ID to string for comparison
        clients_df['Client_ID'] = clients_df['Client_ID'].astype(str)
        
        # Find matching client
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

def ensure_editable_columns_exist(table_name):
    """Ensure Lead_Status, Revenue, and Notes columns exist in the table"""
    client = init_bigquery_client()
    if not client:
        return False
    
    try:
        table_ref = f"{PROJECT_ID}.master.{table_name}"
        table = client.get_table(table_ref)
        
        # Check existing columns
        existing_columns = [field.name for field in table.schema]
        
        # Add Lead_Status column if it doesn't exist
        if 'Lead_Status' not in existing_columns:
            try:
                # Step 1: Add column
                client.query(f"ALTER TABLE `{table_ref}` ADD COLUMN Lead_Status STRING").result()
                # Step 2: Set default
                client.query(f"ALTER TABLE `{table_ref}` ALTER COLUMN Lead_Status SET DEFAULT 'Pending'").result()
                # Step 3: Update existing rows
                client.query(f"UPDATE `{table_ref}` SET Lead_Status = 'Pending' WHERE Lead_Status IS NULL").result()
            except Exception as e:
                pass  # Silently skip if column already exists or can't be added
        
        # Add Revenue column if it doesn't exist
        if 'Revenue' not in existing_columns:
            try:
                # Step 1: Add column
                client.query(f"ALTER TABLE `{table_ref}` ADD COLUMN Revenue FLOAT64").result()
                # Step 2: Set default
                client.query(f"ALTER TABLE `{table_ref}` ALTER COLUMN Revenue SET DEFAULT 0.0").result()
                # Step 3: Update existing rows
                client.query(f"UPDATE `{table_ref}` SET Revenue = 0.0 WHERE Revenue IS NULL").result()
            except Exception as e:
                pass  # Silently skip if column already exists or can't be added
        
        # Add Notes column if it doesn't exist
        if 'Notes' not in existing_columns:
            try:
                # Step 1: Add column
                client.query(f"ALTER TABLE `{table_ref}` ADD COLUMN Notes STRING").result()
                # Step 2: Set default
                client.query(f"ALTER TABLE `{table_ref}` ALTER COLUMN Notes SET DEFAULT ''").result()
                # Step 3: Update existing rows
                client.query(f"UPDATE `{table_ref}` SET Notes = '' WHERE Notes IS NULL").result()
            except Exception as e:
                pass  # Silently skip if column already exists or can't be added
        
        return True
        
    except Exception as e:
        return False

def load_leads_data(table_name, client_id, date_range_type, start_date=None, end_date=None):
    """Load leads data from BigQuery table with date filtering"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        # Build date filter based on selection
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
        
        # Ensure Client_ID is properly quoted/handled as string
        # Check if client_id is numeric or string
        try:
            # Try to convert to int - if it works, use without quotes
            client_id_int = int(client_id)
            client_id_filter = f"Client_ID = {client_id_int}"
        except (ValueError, TypeError):
            # If it's a string (like 'W015'), use quotes
            client_id_filter = f"Client_ID = '{client_id}'"
        
        query = f"""
        SELECT * EXCEPT(year_to_date, month_to_date, quarter_to_date, Client_Name, Client_ID)
        FROM `{PROJECT_ID}.master.{table_name}`
        WHERE {client_id_filter}
        {date_filter}
        ORDER BY date DESC
        """
        
        df = client.query(query).to_dataframe()
        
        # Ensure editable columns exist with proper defaults
        if 'Lead_Status' not in df.columns:
            df['Lead_Status'] = 'Pending'
        else:
            df['Lead_Status'] = df['Lead_Status'].fillna('Pending')
            
        if 'Revenue' not in df.columns:
            df['Revenue'] = 0.0
        else:
            df['Revenue'] = df['Revenue'].fillna(0.0)
            
        if 'Notes' not in df.columns:
            df['Notes'] = ''
        else:
            df['Notes'] = df['Notes'].fillna('')
        
        return df
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def save_leads_data(df, table_name, client_id, date_range_type, start_date=None, end_date=None):
    """Save only the updated rows back to BigQuery, preserving other data"""
    client = init_bigquery_client()
    if not client:
        return False
    
    try:
        table_ref = f"{PROJECT_ID}.master.{table_name}"
        
        # Extract only the columns we need for the update
        # We need lead_id to match records, plus the 3 editable columns
        update_df = df[['lead_id', 'Lead_Status', 'Revenue', 'Notes']].copy()
        
        # Build a temp table with just the update columns
        temp_table = f"{PROJECT_ID}.master.temp_{table_name}_{int(time.time())}"
        
        # Load the edited data to a temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("lead_id", "STRING"),
                bigquery.SchemaField("Lead_Status", "STRING"),
                bigquery.SchemaField("Revenue", "FLOAT64"),
                bigquery.SchemaField("Notes", "STRING"),
            ]
        )
        
        job = client.load_table_from_dataframe(update_df, temp_table, job_config=job_config)
        job.result()
        
        # Use MERGE to update only the editable columns based on lead_id
        merge_query = f"""
        MERGE `{table_ref}` T
        USING `{temp_table}` S
        ON T.lead_id = S.lead_id
        WHEN MATCHED THEN
          UPDATE SET 
            T.Lead_Status = S.Lead_Status,
            T.Revenue = S.Revenue,
            T.Notes = S.Notes
        """
        
        client.query(merge_query).result()
        
        # Clean up temp table
        client.delete_table(temp_table, not_found_ok=True)
        
        return True
        
    except Exception as e:
        st.error(f"Error saving data: {str(e)}")
        # Try to clean up temp table
        try:
            client.delete_table(temp_table, not_found_ok=True)
        except:
            pass
        return False

def calculate_scorecard_metrics(form_df, call_df):
    """Calculate metrics for scorecards"""
    # Ensure Lead_Status column exists in both dataframes
    if not form_df.empty and 'Lead_Status' not in form_df.columns:
        form_df['Lead_Status'] = 'Pending'
    if not call_df.empty and 'Lead_Status' not in call_df.columns:
        call_df['Lead_Status'] = 'Pending'
    
    combined_df = pd.concat([form_df, call_df], ignore_index=True) if not form_df.empty or not call_df.empty else pd.DataFrame()
    
    metrics = {
        'total_leads': len(combined_df),
        'form_leads': len(form_df),
        'call_leads': len(call_df),
        'qualified': len(combined_df[combined_df['Lead_Status'] == 'Qualified']) if not combined_df.empty and 'Lead_Status' in combined_df.columns else 0,
        'scheduled': len(combined_df[combined_df['Lead_Status'] == 'Scheduled']) if not combined_df.empty and 'Lead_Status' in combined_df.columns else 0,
        'appointments': len(combined_df[combined_df['Lead_Status'] == 'Appointment']) if not combined_df.empty and 'Lead_Status' in combined_df.columns else 0,
        'sales': len(combined_df[combined_df['Lead_Status'] == 'Sale']) if not combined_df.empty and 'Lead_Status' in combined_df.columns else 0
    }
    
    return metrics

def display_scorecards(metrics):
    """Display scorecard metrics in styled containers"""
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
    
    # First row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Total Leads</div>
            <div class="scorecard-value">{metrics['total_leads']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Form Leads</div>
            <div class="scorecard-value">{metrics['form_leads']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Call Leads</div>
            <div class="scorecard-value">{metrics['call_leads']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.write("")  # Spacing
    
    # Second row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Qualified Leads</div>
            <div class="scorecard-value">{metrics['qualified']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Scheduled</div>
            <div class="scorecard-value">{metrics['scheduled']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Appointments</div>
            <div class="scorecard-value">{metrics['appointments']}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="scorecard">
            <div class="scorecard-label">Sales</div>
            <div class="scorecard-value">{metrics['sales']}</div>
        </div>
        """, unsafe_allow_html=True)

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "client_name" not in st.session_state:
    st.session_state.client_name = None
if "client_id" not in st.session_state:
    st.session_state.client_id = None
if "form_leads_df" not in st.session_state:
    st.session_state.form_leads_df = pd.DataFrame()
if "call_leads_df" not in st.session_state:
    st.session_state.call_leads_df = pd.DataFrame()
if "form_changes_made" not in st.session_state:
    st.session_state.form_changes_made = False
if "call_changes_made" not in st.session_state:
    st.session_state.call_changes_made = False

# Login page
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
                    st.session_state.client_name = client_name
                    st.session_state.client_id = client_id
                    st.success(f"Welcome, {client_name}!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Invalid username or password")
            else:
                st.warning("Please enter both username and password")
    
    st.stop()

# Main application (after authentication)
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
                value=date.today().replace(day=1),  # First day of current month
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
        st.session_state.client_name = None
        st.session_state.client_id = None
        st.rerun()

# Load data based on date range
with st.spinner("Loading leads data..."):
    # Ensure editable columns exist in both tables
    ensure_editable_columns_exist("all_form_table")
    ensure_editable_columns_exist("all_marchex_table")
    
    st.session_state.form_leads_df = load_leads_data(
    "all_form_table",
    st.session_state.client_id,
    start_date,
    end_date
    )
    
    st.session_state.call_leads_df = load_leads_data(
        "all_marchex_table",
        st.session_state.client_id,
        start_date,
        end_date
    )

# Calculate and display scorecards
metrics = calculate_scorecard_metrics(st.session_state.form_leads_df, st.session_state.call_leads_df)
display_scorecards(metrics)

st.markdown("<br>", unsafe_allow_html=True)

# Tabs
tab1, tab2 = st.tabs(["Form Leads", "Call Leads"])

with tab1:
    st.header("Form Leads")
    
    form_df = st.session_state.form_leads_df.copy()
    
    if not form_df.empty:
        # Count pending statuses
        pending_count = len(form_df[form_df['Lead_Status'] == 'Pending'])
        
        st.write(f"Total Form Leads: `{len(form_df)}` | Pending Lead Statuses: `{pending_count}`")
        
        # Get list of non-editable columns
        editable_cols = ['Lead_Status', 'Revenue', 'Notes']
        all_cols = form_df.columns.tolist()
        disabled_cols = [col for col in all_cols if col not in editable_cols]
        
        # Display editable dataframe
        edited_form_df = st.data_editor(
            form_df,
            use_container_width=True,
            hide_index=True,
            disabled=disabled_cols,
            column_config={
                "lead_id": None,
                "Lead_Status": st.column_config.SelectboxColumn(
                    "Lead Status",
                    options=['Pending', 'Unqualified', 'Qualified', 'Scheduled', 'Appointment', 'Sale'],
                    required=True,
                    default='Pending'
                ),
                "Revenue": st.column_config.NumberColumn(
                    "Revenue",
                    format="$%.2f",
                    min_value=0.0,
                    default=0.0
                ),
                "Notes": st.column_config.TextColumn(
                    "Notes",
                    max_chars=500,
                    default=""
                )
            },
            key="form_leads_editor"
        )
        
        # Check if changes were made
        if not edited_form_df.equals(form_df):
            st.session_state.form_changes_made = True
        
        # Show save button if changes were made
        if st.session_state.form_changes_made:
            if st.button("💾 Save Changes", type="primary", key="save_form_leads"):
                with st.spinner("Saving changes..."):
                    if save_leads_data(edited_form_df, "all_form_table", st.session_state.client_id, date_range_type, start_date, end_date):
                        st.session_state.form_leads_df = edited_form_df
                        st.session_state.form_changes_made = False
                        st.toast("Leads updated successfully!", icon="✅")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.toast("Failed to save changes", icon="❌")
    else:
        st.info("No form leads data available for the selected date range.")

with tab2:
    st.header("Call Leads")
    
    call_df = st.session_state.call_leads_df.copy()
    
    if not call_df.empty:
        # Count pending statuses
        pending_count = len(call_df[call_df['Lead_Status'] == 'Pending'])
        
        st.write(f"Total Call Leads: `{len(call_df)}` | Pending Lead Statuses: `{pending_count}`")
        
        # Get list of non-editable columns
        editable_cols = ['Lead_Status', 'Revenue', 'Notes']
        all_cols = call_df.columns.tolist()
        disabled_cols = [col for col in all_cols if col not in editable_cols]
        
        # Display editable dataframe
        edited_call_df = st.data_editor(
            call_df,
            use_container_width=True,
            hide_index=True,
            disabled=disabled_cols,
            column_config={
                "lead_id": None,
                "Lead_Status": st.column_config.SelectboxColumn(
                    "Lead Status",
                    options=['Pending', 'Unqualified', 'Qualified', 'Scheduled', 'Appointment', 'Sale'],
                    required=True,
                    default='Pending'
                ),
                "Revenue": st.column_config.NumberColumn(
                    "Revenue",
                    format="$%.2f",
                    min_value=0.0,
                    default=0.0
                ),
                "Notes": st.column_config.TextColumn(
                    "Notes",
                    max_chars=500,
                    default=""
                )
            },
            key="call_leads_editor"
        )
        
        # Check if changes were made
        if not edited_call_df.equals(call_df):
            st.session_state.call_changes_made = True
        
        # Show save button if changes were made
        if st.session_state.call_changes_made:
            if st.button("💾 Save Changes", type="primary", key="save_call_leads"):
                with st.spinner("Saving changes..."):
                    if save_leads_data(edited_call_df, "all_marchex_table", st.session_state.client_id, date_range_type, start_date, end_date):
                        st.session_state.call_leads_df = edited_call_df
                        st.session_state.call_changes_made = False
                        st.success("✅ Call leads updated successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to save changes")
    else:
        st.info("No call leads data available for the selected date range.")
