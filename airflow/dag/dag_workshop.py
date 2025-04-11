"""
dag_workshop.py
Optimized ETL DAG for your project structure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Import modules from your project structure
from tasks.extract import (
    extract_spotify_data,
    extract_grammy_data,
    extract_lastfm_data
)
from tasks.transform_spotify import transform_spotify_data
from tasks.transform_grammy import transform_grammy_data
from tasks.transform_api import transform_lastfm_data 
from tasks.merge import merge_datasets
from tasks.load import (
    load_to_postgresql,
    export_to_drive
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
    description='Optimized ETL pipeline for music data',
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Extract Phase - Parallel execution
    with TaskGroup("extract_phase") as extract_group:
        extract_spotify = PythonOperator(
            task_id='extract_spotify',
            python_callable=extract_spotify_data,
            op_kwargs={
                'path': 'data/raw/spotify_dataset.csv',
                'base_dir': Variable.get("project_root", default_var="/opt/airflow")
            }
        )
        
        extract_grammy = PythonOperator(
            task_id='extract_grammy',
            python_callable=extract_grammy_data,
            op_kwargs={
                'path': 'data/raw/grammy.csv',
                'table_name': 'grammy_raw',
                'base_dir': Variable.get("project_root", default_var="/opt/airflow")
            }
        )
        
        extract_lastfm = PythonOperator(
            task_id='extract_lastfm',
            python_callable=extract_lastfm_data,
            op_kwargs={
                'path': 'data/raw/lastfm_data.csv',
                'base_dir': Variable.get("project_root", default_var="/opt/airflow")
            }
        )

    # Transform Phase - Parallel execution
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

    # Load Phase - Parallel execution
    with TaskGroup("load_phase") as load_group:
        load_db = PythonOperator(
            task_id='load_postgresql',
            python_callable=load_to_postgresql,
            op_kwargs={
                'table_name': 'music_analytics_merged',
                'chunk_size': 5000
            }
        )
        
        export_csv = PythonOperator(
            task_id='export_to_drive',
            python_callable=export_to_drive,
            op_kwargs={
                'drive_folder_id': Variable.get("drive_folder_id"),
                'service_account_path': 'tasks/service_account.json'
            }
        )

    # Set up dependencies
    extract_group >> transform_group >> merge_data >> load_group

    # Add documentation
    dag.doc_md = """
    ## ETL Workshop 002 Pipeline
    Customized for your project structure:
    - Extracts from Spotify CSV, Grammy DB, and LastFM API
    - Transforms using separate modules
    - Merges with fuzzy matching
    - Loads to PostgreSQL and Google Drive
    """