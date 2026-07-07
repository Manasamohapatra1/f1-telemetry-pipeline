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

# Fallback to Streamlit secrets if running on Streamlit Cloud
if not db_url and "DATABASE_URL" in st.secrets:
    db_url = st.secrets["DATABASE_URL"]
    
if not db_url:
    st.error("DATABASE_URL is not set. Please set it in .env locally or Streamlit Secrets on the cloud.")
    st.stop()

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

@st.cache_data(ttl=3600)
def load_race_winner(year, event_name):
    try:
        session = fastf1.get_session(year, event_name, 'R')
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        if not session.results.empty:
            winner = session.results.iloc[0]
            headshot = winner.get('HeadshotUrl')
            if pd.isna(headshot) or not str(headshot).startswith('http'):
                headshot = None
            return {
                "full_name": winner.get('FullName', 'Unknown Driver'),
                "team_name": winner.get('TeamName', 'Unknown Team'),
                "driver_number": str(winner.get('DriverNumber', '')),
                "headshot_url": headshot,
                "time": str(winner.get('Time', ''))
            }
    except Exception as e:
        return None
    return None

@st.cache_data(ttl=3600)
def load_pole_position(year, event_name):
    try:
        session = fastf1.get_session(year, event_name, 'Q')
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        if not session.results.empty:
            pole = session.results.iloc[0]
            headshot = pole.get('HeadshotUrl')
            if pd.isna(headshot) or not str(headshot).startswith('http'):
                headshot = None
            time_str = "N/A"
            for q_col in ['Q3', 'Q2', 'Q1', 'Time']:
                if q_col in pole and pd.notnull(pole[q_col]):
                    time_str = str(pole[q_col])
                    break
            return {
                "full_name": pole.get('FullName', 'Unknown Driver'),
                "team_name": pole.get('TeamName', 'Unknown Team'),
                "driver_number": str(pole.get('DriverNumber', '')),
                "headshot_url": headshot,
                "time": time_str
            }
    except Exception as e:
        return None
    return None

@st.cache_data(ttl=3600)
def load_fastest_lap_info(year, event_name, _laps_df, _drivers_df):
    try:
        if _laps_df.empty or 'lap_time_ms' not in _laps_df.columns:
            return None
        valid_laps = _laps_df[_laps_df['lap_time_ms'] > 0]
        if valid_laps.empty:
            return None
        fastest_row = valid_laps.loc[valid_laps['lap_time_ms'].idxmin()]
        fl_driver_id = fastest_row['driver_id']
        fl_time_ms = fastest_row['lap_time_ms']
        fl_lap_num = fastest_row['lap_number']
        
        fl_seconds = fl_time_ms / 1000.0
        fl_min = int(fl_seconds // 60)
        fl_sec = fl_seconds % 60
        fl_time_str = f"{fl_min:02d}:{fl_sec:06.3f} (Lap {fl_lap_num})"
        
        d_row = _drivers_df[_drivers_df['driver_id'] == fl_driver_id]
        if not d_row.empty:
            full_name = d_row.iloc[0]['full_name']
            team_name = d_row.iloc[0]['team_name']
            driver_number = str(d_row.iloc[0]['driver_number'])
        else:
            full_name = fl_driver_id
            team_name = "Unknown Team"
            driver_number = ""
            
        headshot = None
        try:
            session = fastf1.get_session(year, event_name, 'R')
            session.load(laps=False, telemetry=False, weather=False, messages=False)
            if not session.results.empty:
                res_row = session.results[session.results['Abbreviation'] == fl_driver_id]
                if not res_row.empty:
                    hs = res_row.iloc[0].get('HeadshotUrl')
                    if pd.notna(hs) and str(hs).startswith('http'):
                        headshot = hs
        except:
            pass
            
        return {
            "full_name": full_name,
            "team_name": team_name,
            "driver_number": driver_number,
            "headshot_url": headshot,
            "time": fl_time_str
        }
    except Exception as e:
        return None

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

# Display Race Highlights Cards (Race Winner, Pole Position & Fastest Lap)
winner_info = load_race_winner(current_year, selected_event)
pole_info = load_pole_position(current_year, selected_event)
fastest_lap_info = load_fastest_lap_info(current_year, selected_event, laps_df, drivers_df)

if winner_info or pole_info or fastest_lap_info:
    col1, col2, col3 = st.columns(3)
    with col1:
        if winner_info:
            with st.container():
                w_col1, w_col2 = st.columns([1, 2.5])
                with w_col1:
                    if winner_info["headshot_url"]:
                        st.image(winner_info["headshot_url"], width=85)
                    else:
                        st.markdown("### 🏆")
                with w_col2:
                    st.markdown(f"**🏆 Winner**\n\n**{winner_info['full_name']}** (#{winner_info['driver_number']})\n\n*{winner_info['team_name']}*")
                    if winner_info["time"] and winner_info["time"] not in ["NaT", "None", ""]:
                        st.caption(f"Time: `{winner_info['time']}`")
    with col2:
        if pole_info:
            with st.container():
                p_col1, p_col2 = st.columns([1, 2.5])
                with p_col1:
                    if pole_info["headshot_url"]:
                        st.image(pole_info["headshot_url"], width=85)
                    else:
                        st.markdown("### ⚡")
                with p_col2:
                    st.markdown(f"**⚡ Pole Position**\n\n**{pole_info['full_name']}** (#{pole_info['driver_number']})\n\n*{pole_info['team_name']}*")
                    if pole_info["time"] and pole_info["time"] not in ["NaT", "None", "", "N/A"]:
                        st.caption(f"Lap: `{pole_info['time']}`")
    with col3:
        if fastest_lap_info:
            with st.container():
                f_col1, f_col2 = st.columns([1, 2.5])
                with f_col1:
                    if fastest_lap_info["headshot_url"]:
                        st.image(fastest_lap_info["headshot_url"], width=85)
                    else:
                        st.markdown("### 🟣")
                with f_col2:
                    st.markdown(f"**🟣 Fastest Lap**\n\n**{fastest_lap_info['full_name']}** (#{fastest_lap_info['driver_number']})\n\n*{fastest_lap_info['team_name']}*")
                    if fastest_lap_info["time"]:
                        st.caption(f"Lap Time: `{fastest_lap_info['time']}`")
    st.divider()

st.sidebar.subheader("Compare Fastest Laps")
# By default, select the top 2 fastest drivers
default_drivers = laps_df.groupby('driver_id')['lap_time_ms'].min().sort_values().head(2).index.tolist()

# Only allow selecting drivers who actually participated in this specific race
valid_race_drivers = laps_df['driver_id'].unique().tolist()
# Sort alphabetically for better UX
valid_race_drivers.sort()

selected_drivers = st.sidebar.multiselect(
    "Select Drivers", 
    options=valid_race_drivers,
    default=[d for d in default_drivers if d in valid_race_drivers]
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
