"""
load_and_export_data.py
Script for loading merged data to PostgreSQL and exporting to Google Drive
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine
from airflow.exceptions import AirflowException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io


def load_and_export_data(**kwargs) -> None:
    """
    Main function to:
    1. Load merged data to PostgreSQL
    2. Export final CSV to Google Drive

    Args:
        kwargs: Airflow context

    Raises:
        AirflowException: If any step fails
    """
    try:
        ti = kwargs['ti']
        merged_df = ti.xcom_pull(task_ids='merge_datasets_task')

        if merged_df is None or merged_df.empty:
            raise AirflowException("No merged data received")

        load_to_postgresql(merged_df)
        export_to_drive(merged_df)

    except Exception as e:
        raise AirflowException(f"Data loading/export failed: {str(e)}")


def load_to_postgresql(df: pd.DataFrame) -> None:
    """Load DataFrame to PostgreSQL database"""
    try:
        with open(os.path.join(os.getcwd(), "credentials.json"), "r") as file:
            credentials = json.load(file)

        engine = create_engine(
            f"postgresql://{credentials['db_user']}:{credentials['db_password']}@"
            f"{credentials['db_host']}:5432/{credentials['db_name']}?client_encoding=utf8"
        )

        with engine.begin() as connection:
            df.to_sql(
                'artists_analytics_merged',
                connection,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )

        print(f"Successfully loaded {len(df)} records to PostgreSQL")

    except Exception as e:
        raise AirflowException(f"PostgreSQL load failed: {str(e)}")


def export_to_drive(df: pd.DataFrame) -> None:
    """Export DataFrame to Google Drive as CSV"""
    try:
        DRIVE_FOLDER_ID = "1ize2-haamshym7tY8wJ5efd648coydZ1"
        SERVICE_ACCOUNT_FILE = os.path.join(os.getcwd(), "service_account.json")

        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise AirflowException(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")

        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': 'merged_music_data.csv',
            'parents': [DRIVE_FOLDER_ID]
        }

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        media = MediaIoBaseUpload(csv_buffer, mimetype='text/csv', resumable=True)

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink'
        ).execute()

        print("✅ Archivo subido exitosamente a Google Drive")
        print(f"📎 ID: {file.get('id')}")
        print(f"🔗 Enlace: {file.get('webViewLink')}")

    except Exception as e:
        raise AirflowException(f"Google Drive export failed: {str(e)}")
