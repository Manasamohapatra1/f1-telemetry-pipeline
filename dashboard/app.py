import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import fastf1

# Load env variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
db_url = os.getenv("DATABASE_URL")

st.set_page_config(page_title="F1 Telemetry Dashboard", layout="wide", page_icon="🏎️")

from sqlalchemy.pool import NullPool

@st.cache_resource
def init_connection():
    # NullPool prevents connection pooling which avoids stale connection and PendingRollbackError issues
    return create_engine(db_url, poolclass=NullPool)

engine = init_connection()

@st.cache_data(ttl=600)
def load_races():
    return pd.read_sql("SELECT DISTINCT race_id, race_name, year FROM dim_races", con=engine)

@st.cache_data(ttl=3600)
def load_schedule(year=2025):
    # Fetch all events for the year
    schedule = fastf1.get_event_schedule(year)
    # Filter out non-race events if any, usually RoundNumber > 0 are actual races
    return schedule[schedule['RoundNumber'] > 0]

@st.cache_data(ttl=600)
def load_drivers():
    return pd.read_sql("SELECT * FROM dim_drivers", con=engine)

@st.cache_data(ttl=600)
def load_laps(race_id):
    return pd.read_sql(f"SELECT * FROM fct_laps WHERE race_id = '{race_id}'", con=engine)

@st.cache_data(ttl=600)
def load_telemetry(race_id, driver_ids):
    if not driver_ids:
        return pd.DataFrame()
    
    driver_str = "','".join(driver_ids)
    
    # Query to fetch the telemetry for the selected drivers
    # Because of the 10x optimization, the database now ONLY contains the fastest lap telemetry
    query = f"""
    SELECT *
    FROM fct_telemetry
    WHERE race_id = '{race_id}' AND driver_id IN ('{driver_str}')
    ORDER BY distance ASC
    """
    return pd.read_sql(query, con=engine)

# ==========================================
# DASHBOARD UI
# ==========================================

st.title("🏎️ Formula 1 Telemetry Dashboard")
st.markdown("Analyze high-frequency telemetry data from the Neon PostgreSQL data warehouse.")

races = load_races()

# Set current year
current_year = 2025
schedule = load_schedule(current_year)

st.sidebar.header("Controls")
selected_event = st.sidebar.selectbox(
    f"Select {current_year} Race", 
    options=schedule['EventName'].tolist()
)

# Calculate what the race_id would be based on how extract.py does it
expected_race_id = f"{current_year}_{selected_event.replace(' ', '')}"

# Check if it exists in the database
is_cached = not races.empty and expected_race_id in races['race_id'].values

if not is_cached:
    st.warning(f"The data for {selected_event} is not in the database.")
    if st.button(f"⬇️ Download {selected_event} Telemetry", use_container_width=True):
        with st.spinner("Triggering Airflow Pipeline..."):
            url = "http://localhost:8080/api/v1/dags/f1_telemetry_pipeline/dagRuns"
            payload = {
                "conf": {
                    "year": current_year,
                    "race_name": selected_event
                }
            }
            # Default Airflow credentials
            auth = HTTPBasicAuth('airflow', 'airflow')
            
            try:
                # Trigger DAG
                response = requests.post(url, json=payload, auth=auth)
                if response.status_code == 200:
                    dag_run_id = response.json()['dag_run_id']
                    
                    # Poll Airflow until completion
                    status = "queued"
                    progress_text = "Downloading telemetry for all 20 drivers... (This takes about 20 seconds. Please do not close this page.)"
                    status_placeholder = st.empty()
                    
                    while status in ["queued", "running"]:
                        status_placeholder.info(f"{progress_text} Current Status: {status.upper()}")
                        time.sleep(10) # Poll every 10 seconds
                        
                        poll_url = f"{url}/{dag_run_id}"
                        poll_resp = requests.get(poll_url, auth=auth)
                        if poll_resp.status_code == 200:
                            status = poll_resp.json()['state']
                            
                    if status == "success":
                        st.success("Data downloaded successfully! Refreshing dashboard...")
                        time.sleep(2)
                        # Clear the Streamlit cache so load_races() hits the database and sees the new race
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"Pipeline failed with status: {status}")
                else:
                    st.error(f"Failed to trigger Airflow. Status Code: {response.status_code}. Make sure Airflow is running.")
            except requests.exceptions.ConnectionError:
                st.error("Airflow is not running. Since this is the Cloud Showcase Version, you can only view pre-downloaded races. Please select another race from the dropdown, or run Airflow locally to download this race.")
            except Exception as e:
                st.error(f"Error communicating with Airflow: {e}")
    st.stop()

# If we get here, data is cached!
selected_race_id = expected_race_id

drivers_df = load_drivers()
laps_df = load_laps(selected_race_id)

st.sidebar.subheader("Compare Fastest Laps")
# By default, select the top 2 fastest drivers
default_drivers = laps_df.groupby('driver_id')['lap_time_ms'].min().sort_values().head(2).index.tolist()

selected_drivers = st.sidebar.multiselect(
    "Select Drivers", 
    options=drivers_df['driver_id'].tolist(),
    default=default_drivers
)

if not selected_drivers:
    st.warning("Please select at least one driver.")
    st.stop()

# Load telemetry for selected drivers
with st.spinner("Querying thousands of telemetry data points from Neon Postgres..."):
    telemetry_df = load_telemetry(selected_race_id, selected_drivers)

if telemetry_df.empty:
    st.warning("No telemetry data found for the selected drivers.")
    st.stop()

st.subheader("Speed Trace (Fastest Lap)")
st.markdown("This chart compares the speed of the selected drivers across the entire track distance during their single fastest lap.")

fig_speed = px.line(
    telemetry_df, 
    x="distance", 
    y="speed", 
    color="driver_id",
    labels={"distance": "Distance (meters)", "speed": "Speed (km/h)", "driver_id": "Driver"},
    template="plotly_dark",
    color_discrete_sequence=px.colors.qualitative.Set1
)
fig_speed.update_layout(hovermode="x unified")
st.plotly_chart(fig_speed, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Throttle Application")
    fig_throttle = px.line(
        telemetry_df, 
        x="distance", 
        y="throttle", 
        color="driver_id",
        labels={"distance": "Distance (meters)", "throttle": "Throttle %", "driver_id": "Driver"},
        template="plotly_dark",
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    st.plotly_chart(fig_throttle, use_container_width=True)

with col2:
    st.subheader("Braking")
    fig_brake = px.line(
        telemetry_df, 
        x="distance", 
        y="brake", 
        color="driver_id",
        labels={"distance": "Distance (meters)", "brake": "Brake Pressure %", "driver_id": "Driver"},
        template="plotly_dark",
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    st.plotly_chart(fig_brake, use_container_width=True)

st.subheader("Lap Time Distribution")
# Filter out extremely slow laps (e.g. out laps, pit laps, safety cars) for better box plot visualization
# Let's say any lap more than 130% of the fastest lap is an outlier for this view
min_lap = laps_df['lap_time_ms'].min()
valid_laps = laps_df[laps_df['lap_time_ms'] < (min_lap * 1.3)]

fig_box = px.box(
    valid_laps[valid_laps['driver_id'].isin(selected_drivers)], 
    x="driver_id", 
    y="lap_time_ms", 
    color="driver_id",
    labels={"lap_time_ms": "Lap Time (milliseconds)", "driver_id": "Driver"},
    template="plotly_dark",
    color_discrete_sequence=px.colors.qualitative.Set1
)
st.plotly_chart(fig_box, use_container_width=True)
