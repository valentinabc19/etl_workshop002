"""
Data extraction module for Airflow DAG.
Handles extraction from CSV files and loading to PostgreSQL.
"""

import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_grammy_data(path, base_dir=None) -> pd.DataFrame:
    """
    Extracts Grammy Awards data from a CSV file and loads it into a PostgreSQL database table.

    Args:
        path: Relative path to the CSV file
        base_dir: Base directory for paths (defaults to current directory)

    Returns:
        pd.DataFrame: A DataFrame containing the loaded Grammy Awards data.
    """
    if base_dir is None:
        base_dir = os.getcwd()

    with open(os.path.join(base_dir, "credentials.json"), "r", encoding="utf-8") as file:
        credentials = json.load(file)

    db_host = credentials["db_host"]
    db_name = credentials["db_name"]
    db_user = credentials["db_user"]
    db_password = credentials["db_password"]

    csv_path = os.path.join(base_dir, path)
    grammys_data = pd.read_csv(csv_path, sep=",")

    pg_engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}?client_encoding=utf8")

    grammys_data.to_sql('grammy_raw_data', pg_engine, if_exists="replace", index=False)
    
            
    return pd.read_sql(f"SELECT * FROM grammy_raw_data;", pg_engine)

def extract_spotify_data(path: str, base_dir: Optional[str] = None) -> pd.DataFrame:
    """Extracts Spotify streaming data from CSV.

    Args:
        path: Relative path to the CSV file
        base_dir: Base directory for paths (defaults to current directory)
        required_columns: List of columns that must be present

    Returns:
        Loaded Spotify data as DataFrame

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If required columns are missing
        RuntimeError: If extraction fails
    """
    try:
        base_dir = base_dir or os.getcwd()
        csv_path = os.path.join(base_dir, path)
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info(f"Loading Spotify data from {csv_path}")
        spotify_data = pd.read_csv(
            csv_path,
            encoding='utf-8'
        )
        
        logger.info(f"Successfully loaded {len(spotify_data)} records")
        return spotify_data

    except Exception as e:
        logger.error(f"Spotify data extraction failed: {str(e)}")
        raise RuntimeError(f"Spotify data extraction failed: {str(e)}")