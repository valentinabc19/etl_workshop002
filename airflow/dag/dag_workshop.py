"""
dag_workshop.py
Fixed ETL DAG for your project structure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os

# Import modules using absolute paths
try:
    from tasks.extract import (
        extract_spotify_data,
        extract_grammy_data,
        extract_lastfm_data
    )
    from tasks.transform_spotify import transform_spotify_data
    from tasks.transform_grammy import transform_grammy_data
    from tasks.transform_apl import transform_lastfm_data
    from tasks.merge import merge_datasets
    from tasks.load import (
        load_to_postgresql,
        export_to_drive
    )
except ImportError as e:
    raise ImportError(f"Failed to import modules: {str(e)}. Check your tasks/ directory structure.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

# Get absolute path to project root
try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except Exception as e:
    raise RuntimeError(f"Could not determine project root: {str(e)}")

with DAG(
    'etl_workshop002',
    default_args=default_args,
    description='Optimized ETL pipeline for music data',
    schedule_interval=None,  # Changed from '@weekly' to None for debugging
    catchup=False,
    max_active_runs=1,
) as dag:

    # Extract Phase
    with TaskGroup("extract_phase") as extract_group:
        extract_spotify = PythonOperator(
            task_id='extract_spotify',
            python_callable=extract_spotify_data,
            op_kwargs={
                'path': 'data/raw/spotify_dataset.csv',
                'base_dir': PROJECT_ROOT  # Using absolute path
            }
        )
        
        extract_grammy = PythonOperator(
            task_id='extract_grammy',
            python_callable=extract_grammy_data,
            op_kwargs={
                'path': 'data/raw/grammy.csv',
                'table_name': 'grammy_raw',
                'base_dir': PROJECT_ROOT
            }
        )
        
        extract_lastfm = PythonOperator(
            task_id='extract_lastfm',
            python_callable=extract_lastfm_data,
            op_kwargs={
                'path': 'data/raw/lastfm_data.csv',
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
                'chunk_size': 500,
                'credentials_path': os.path.join(PROJECT_ROOT, 'tasks', 'credentials.json')
            }
        )
        
        export_csv = PythonOperator(
            task_id='export_to_drive',
            python_callable=export_to_drive,
            op_kwargs={
                'drive_folder_id': Variable.get("drive_folder_id", default_var=""),
                'service_account_path': os.path.join(PROJECT_ROOT, 'tasks', 'service_account.json')
            }
        )

    # Set up dependencies
    extract_group >> transform_group >> merge_data >> load_group

    # Add documentation
    dag.doc_md = f"""
    ## ETL Workshop 002 Pipeline
    Project Root: {PROJECT_ROOT}
    Data Paths:
    - Spotify: {os.path.join(PROJECT_ROOT, 'data/raw/spotify_dataset.csv')}
    - Grammy: {os.path.join(PROJECT_ROOT, 'data/raw/grammy.csv')}
    - LastFM: {os.path.join(PROJECT_ROOT, 'data/raw/lastfm_data.csv')}
    """