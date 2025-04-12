"""
dag_workshop.py
Final working version with absolute imports
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Configure Python path
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
AIRFLOW_DIR = os.path.dirname(DAG_DIR)
PROJECT_ROOT = os.path.dirname(AIRFLOW_DIR)
sys.path.append(AIRFLOW_DIR)  # Add airflow/ directory to path

# Absolute imports
try:
    from tasks.extract import (
        extract_spotify_data,
        extract_grammy_data
    )
    from tasks.extract_api import extract_lastfm_data
    from tasks.transform_spotify import transform_spotify_data
    from tasks.transform_grammy import transform_grammy_data
    from tasks.transform_api import transform_lastfm_data
    from tasks.merge import merge_datasets
    from tasks.load import (
        load_to_postgresql,
        export_to_drive
    )
except ImportError as e:
    raise ImportError(
        f"Import failed: {str(e)}\n"
        f"Current directory: {os.getcwd()}\n"
        f"DAG_DIR: {DAG_DIR}\n"
        f"AIRFLOW_DIR: {AIRFLOW_DIR}\n"
        f"PROJECT_ROOT: {PROJECT_ROOT}\n"
        f"Python path: {sys.path}\n"
        f"Contents of tasks dir: {os.listdir(os.path.join(AIRFLOW_DIR, 'tasks'))}"
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'etl_workshop002',
    default_args=default_args,
    description='ETL pipeline for music data',
    schedule_interval=None,  # Manual triggers for debugging
    catchup=False,
) as dag:

    # Extract Phase
    with TaskGroup("extract_phase") as extract_group:
        extract_spotify = PythonOperator(
            task_id='extract_spotify',
            python_callable=extract_spotify_data,
            op_kwargs={
                'path': os.path.join('data', 'raw', 'spotify_dataset.csv'),
                'base_dir': PROJECT_ROOT
            }
        )
        
        extract_grammy = PythonOperator(
            task_id='extract_grammy',
            python_callable=extract_grammy_data,
            op_kwargs={
                'path': os.path.join('data', 'raw', 'the_grammy_awards.csv'),
                'table_name': 'grammy_raw',
                'base_dir': PROJECT_ROOT
            }
        )
        
        extract_lastfm = PythonOperator(
            task_id='extract_lastfm',
            python_callable=extract_lastfm_data,
            op_kwargs={
                'path': os.path.join('data', 'raw', 'lastfm_data.csv'),
                'base_dir': PROJECT_ROOT
            }
        )

    # Transform Phase
    with TaskGroup("transform_phase") as transform_group:
        transform_spotify = PythonOperator(
            task_id='transform_spotify',
            python_callable=transform_spotify_data
        )
        
        transform_grammy = PythonOperator(
            task_id='transform_grammy',
            python_callable=transform_grammy_data
        )
        
        transform_lastfm = PythonOperator(
            task_id='transform_lastfm',
            python_callable=transform_lastfm_data
        )

    # Merge Phase
    merge_data = PythonOperator(
        task_id='merge_datasets',
        python_callable=merge_datasets
    )

    # Load Phase
    with TaskGroup("load_phase") as load_group:
        load_db = PythonOperator(
            task_id='load_postgresql',
            python_callable=load_to_postgresql,
            op_kwargs={
                'table_name': 'music_analytics_merged',
                'credentials_path': os.path.join(AIRFLOW_DIR, 'tasks', 'credentials.json')
            }
        )
        
        export_csv = PythonOperator(
            task_id='export_to_drive',
            python_callable=export_to_drive,
            op_kwargs={
                'drive_folder_id': Variable.get("drive_folder_id", default_var=""),
                'service_account_path': os.path.join(AIRFLOW_DIR, 'tasks', 'service_account.json')
            }
        )

    # Set up dependencies
    extract_group >> transform_group >> merge_data >> load_group