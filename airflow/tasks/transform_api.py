"""
Last.fm Data Transformation Module for Airflow DAG
Handles cleaning and transformation of Last.fm API data
"""

import pandas as pd
import numpy as np
from airflow.exceptions import AirflowException

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to standardized names
    
    Args:
        df: Raw DataFrame from Last.fm API
        
    Returns:
        DataFrame with renamed columns
    """
    try:
        df = df.copy()
        df["artist"] = df["artist_name"]
        df["lastfm_listeners"] = df["listeners"]
        df["lastfm_playcount"] = df["playcount"]
        return df
    except KeyError as e:
        raise AirflowException(f"Column rename failed: {str(e)}")

def extract_similar_artists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract similar artists into separate columns
    
    Args:
        df: DataFrame containing 'similar' column
        
    Returns:
        DataFrame with similar artists split into columns
    """
    try:
        df = df.copy()
        df['similar'] = df['similar'].replace('', np.nan)
        
        # Split similar artists string into separate columns
        split_similar = df['similar'].dropna().str.split(';', n=2, expand=True)
        split_similar = split_similar.apply(lambda col: col.str.strip())
        
        # Assign to new columns
        df[['similar_1', 'similar_2', 'similar_3']] = split_similar
        return df
    except Exception as e:
        raise AirflowException(f"Similar artists extraction failed: {str(e)}")

def standardize_artist_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize artist names by cleaning whitespace and special characters
    
    Args:
        df: DataFrame containing artist names
        
    Returns:
        DataFrame with standardized artist names
    """
    try:
        df = df.copy()
        df['artist'] = (
            df['artist']
            .str.strip()  
            .str.replace(r'[\"]', '', regex=True) 
            .str.replace(r'\s+', ' ', regex=True) 
        )
        return df
    except Exception as e:
        raise AirflowException(f"Artist name standardization failed: {str(e)}")

def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns that are no longer needed
    
    Args:
        df: DataFrame with raw and transformed columns
        
    Returns:
        DataFrame with unnecessary columns removed
    """
    try:
        return df.drop(columns=["artist_name", "similar", "listeners", "playcount"])
    except Exception as e:
        raise AirflowException(f"Column drop failed: {str(e)}")

def transform_lastfm_data(**kwargs) -> pd.DataFrame:
    """
    Main transformation function for Airflow DAG
    Applies all transformations in sequence:
    1. Rename columns
    2. Extract similar artists
    3. Standardize artist names
    4. Drop unused columns
    
    Args:
        kwargs: Airflow context
        
    Returns:
        Fully transformed DataFrame
    """
    try:
        # Get DataFrame from previous task via XCom
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_lastfm_data')
        
        if df is None or df.empty:
            raise AirflowException("No data received from extraction task")
            
        # Apply transformations in sequence
        df = rename_columns(df)
        df = extract_similar_artists(df)
        df = standardize_artist_column(df)
        df = drop_unused_columns(df)
        
        return df
        
    except Exception as e:
        raise AirflowException(f"Data transformation failed: {str(e)}")