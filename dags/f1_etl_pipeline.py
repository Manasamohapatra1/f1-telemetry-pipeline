from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

# Ensure the custom scripts directory is in the Python path
sys.path.insert(0, '/opt/airflow/scripts')

from extract import extract_and_transform
from load import load_data_to_postgres

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def etl_f1_telemetry(**kwargs):
    """
    Combines the extraction, transformation, and loading into a single task.
    We do this in a single task because passing massive DataFrames (like high frequency telemetry)
    between tasks using Airflow XComs is an anti-pattern and can crash the Airflow metadata database.
    """
    dag_run = kwargs.get('dag_run')
    
    # Default to 2023 Bahrain if no config is provided
    year = 2023
    race_name = 'Bahrain'
    
    if dag_run and dag_run.conf:
        year = dag_run.conf.get('year', 2023)
        race_name = dag_run.conf.get('race_name', 'Bahrain')
        
    logger.info(f"Starting ETL for {year} {race_name}")
    
    # 1. Extract and Transform
    races, drivers, laps, telemetry = extract_and_transform(year, race_name)
    
    # 2. Load
    load_data_to_postgres(races, drivers, laps, telemetry)
    
    logger.info("ETL process completed successfully!")

# Define the DAG
with DAG(
    'f1_telemetry_pipeline',
    default_args=default_args,
    description='Extracts F1 telemetry using fastf1 and loads it into a Neon PostgreSQL Database',
    schedule_interval=None, # Set to None for manual triggering
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['f1', 'etl'],
) as dag:

    # Airflow allows passing parameters dynamically in the UI!
    # Parameters are extracted inside the function from kwargs['dag_run'].conf
    run_etl_task = PythonOperator(
        task_id='extract_transform_load_f1_data',
        python_callable=etl_f1_telemetry,
    )
    
    run_etl_task
