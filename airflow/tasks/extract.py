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


def get_database_connection(base_dir: str) -> create_engine:
    """Helper function to create database connection engine.
    
    Args:
        base_dir: Directory containing credentials.json
        
    Returns:
        Initialized SQLAlchemy engine
        
    Raises:
        RuntimeError: If credentials are invalid or connection fails
    """
    try:
        credentials_path = os.path.join(base_dir, "credentials.json")
        with open(credentials_path, "r", encoding="utf-8") as file:
            credentials = json.load(file)
        
        return create_engine(
            f"postgresql://{credentials['db_user']}:{credentials['db_password']}@"
            f"{credentials['db_host']}:5432/{credentials['db_name']}?"
            f"client_encoding=utf8&connect_timeout=10"
        )
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise RuntimeError("Failed to establish database connection")


def extract_grammy_data(path: str, table_name: str, base_dir: Optional[str] = None, if_exists: str = "replace") -> pd.DataFrame:
    """Extracts Grammy awards data from CSV and loads to PostgreSQL.

    Args:
        path: Relative path to the CSV file
        table_name: Target table name in PostgreSQL
        base_dir: Base directory for paths (defaults to current directory)
        if_exists: Behavior when table exists ('replace', 'append', 'fail')

    Returns:
        Data loaded from PostgreSQL for validation

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If invalid parameters provided
        RuntimeError: If extraction or loading fails
    """
    try:
        if if_exists not in ('replace', 'append', 'fail'):
            raise ValueError("if_exists must be 'replace', 'append', or 'fail'")

        base_dir = base_dir or os.getcwd()
        csv_path = os.path.join(base_dir, path)
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info(f"Loading Grammy data from {csv_path}")
        grammys_data = pd.read_csv(csv_path, sep=",", encoding='utf-8')
        
        if grammys_data.empty:
            logger.warning("Empty DataFrame loaded from CSV")
        
        pg_engine = get_database_connection(base_dir)
        
        grammys_data.to_sql(table_name, pg_engine, if_exists="replace", index=False)
            
        return pd.read_sql(f"SELECT * FROM {table_name};", pg_engine)
            
    except Exception as e:
        logger.error(f"Grammy data extraction failed: {str(e)}")
        raise RuntimeError(f"Grammy data extraction failed: {str(e)}")


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