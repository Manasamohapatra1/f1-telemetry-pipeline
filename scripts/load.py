import os
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import psycopg2.extras

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file (useful for local testing outside of Docker)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

def insert_dataframe(df, table_name, engine):
    if df.empty:
        return
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            columns = ','.join(df.columns)
            # Handle NaN/NaT values
            df_clean = df.replace({np.nan: None})
            values = [tuple(x) for x in df_clean.to_numpy()]
            insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_stmt, values, page_size=10000)
        raw_conn.commit()
    finally:
        raw_conn.close()

def load_data_to_postgres(dim_races: pd.DataFrame, dim_drivers: pd.DataFrame, fct_laps: pd.DataFrame, fct_telemetry: pd.DataFrame):
    """
    Loads F1 telemetry DataFrames into the PostgreSQL database.
    Implements a Delete-then-Insert strategy for facts, and Upsert for dimensions to guarantee idempotency.
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found in environment variables. Make sure your .env is loaded.")
        
    engine = create_engine(db_url)
    
    if dim_races.empty:
        logger.warning("No race data provided to load.")
        return

    race_id = dim_races.iloc[0]['race_id']

    # Use a single transaction. If anything fails, it rolls back automatically!
    with engine.begin() as conn:
        logger.info(f"Starting transactional load for race: {race_id}")
        
        # 1. CLEANUP (Idempotency)
        # Delete existing facts and races for THIS specific race to avoid duplicates, but keep other races intact
        logger.info(f"Wiping previous data for {race_id} to maintain idempotency...")
        conn.execute(text("DELETE FROM fct_telemetry WHERE race_id = :race_id"), {"race_id": race_id})
        conn.execute(text("DELETE FROM fct_laps WHERE race_id = :race_id"), {"race_id": race_id})
        conn.execute(text("DELETE FROM dim_races WHERE race_id = :race_id"), {"race_id": race_id})
        
        # 2. UPSERT DRIVERS
        # Drivers persist across races. We upsert them (update if they already exist).
        logger.info("Loading dim_drivers (Upsert)...")
        for _, row in dim_drivers.iterrows():
            insert_driver_sql = """
                INSERT INTO dim_drivers (driver_id, driver_number, full_name, team_name)
                VALUES (:driver_id, :driver_number, :full_name, :team_name)
                ON CONFLICT (driver_id) DO UPDATE SET 
                    driver_number = EXCLUDED.driver_number,
                    full_name = EXCLUDED.full_name,
                    team_name = EXCLUDED.team_name;
            """
            conn.execute(text(insert_driver_sql), {
                "driver_id": row['driver_id'],
                "driver_number": int(row['driver_number']),
                "full_name": row['full_name'],
                "team_name": row['team_name']
            })
            
    # 3. INSERT RACE
    logger.info("Loading dim_races...")
    insert_dataframe(dim_races, 'dim_races', engine)
    
    # 4. INSERT LAPS
    logger.info(f"Loading fct_laps ({len(fct_laps)} rows)...")
    insert_dataframe(fct_laps, 'fct_laps', engine)
        
    # 5. INSERT TELEMETRY
    logger.info(f"Loading fct_telemetry ({len(fct_telemetry)} rows)...")
    insert_dataframe(fct_telemetry, 'fct_telemetry', engine)
        
    logger.info("Data load completed successfully!")

if __name__ == "__main__":
    # This block allows you to run the script standalone for testing.
    # In production (Step 6), Airflow will import the load_data_to_postgres function directly.
    logger.info("This is a module for Airflow. Run the DAG to execute the ETL process.")
