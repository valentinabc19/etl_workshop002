"""
Grammy Data Transformation Module for Airflow DAG
Handles cleaning and transformation of Grammy Awards data
"""

import re
import pandas as pd
from airflow.exceptions import AirflowException

def transform_grammy_data(**kwargs) -> pd.DataFrame:
    """
    Main transformation function for Grammy data in Airflow DAG
    
    Args:
        kwargs: Airflow context
        
    Returns:
        Tuple containing:
        - Transformed DataFrame with all artist information
        - Artist summary DataFrame with nomination statistics
        
    Raises:
        AirflowException: If transformation fails at any step
    """
    try:
        # Get DataFrame from previous task via XCom
        ti = kwargs['ti']
        grammy_df = ti.xcom_pull(task_ids='extract_phase.extract_grammy')
        
        if grammy_df is None or grammy_df.empty:
            raise AirflowException("No data received from extraction task")
            
        # Make a copy to avoid modifying the original
        grammy_df = grammy_df.copy()
        
        # Step 1: Initial column cleanup
        grammy_df = _initial_column_cleanup(grammy_df)
        
        # Step 2: Handle missing artist data
        grammy_df = _handle_missing_artists(grammy_df)
        
        # Step 3: Normalize artist text
        grammy_df = _normalize_artist_text(grammy_df)
        
        # Step 4: Extract primary and featured artists
        grammy_df = _extract_artists(grammy_df)
        
        # Step 5: Classify award categories
        grammy_df = _classify_award_categories(grammy_df)
        
        # Step 6: Create artist summary
        artist_grammy_nominations = _create_artist_summary(grammy_df)
        
        return artist_grammy_nominations
        
    except Exception as e:
        raise AirflowException(f"Grammy data transformation failed: {str(e)}")

def _initial_column_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Remove unnecessary columns and rename winner column"""
    columns_to_drop = ['title', 'published_at', 'updated_at', 'img']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Rename winner column
    if 'winner' in df.columns:
        df["grammy_nominated"] = df["winner"]
        df = df.drop(columns=['winner'])
    
    return df

def _handle_missing_artists(df: pd.DataFrame) -> pd.DataFrame:
    """Handle cases where artist information is missing"""
    # Classical categories where nominee is the artist
    classical_categories = [
        "Best Classical Vocal Soloist Performance",
        "Best Classical Vocal Performance",
        "Best Small Ensemble Performance (With Or Without Conductor)",
        "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
        "Most Promising New Classical Recording Artist",
        "Best Classical Performance - Vocal Soloist (With Or Without Orchestra)",
        "Best New Classical Artist",
        "Best Classical Vocal Soloist",
        "Best Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
        "Best Classical Performance - Vocal Soloist"
    ]
    
    # Identify rows with missing artist info
    both_null = df.loc[df["artist"].isna() & df["workers"].isna()].copy()
    both_null_filtered = both_null[both_null["category"].isin(classical_categories)]
    
    # Update artist from nominee for classical categories
    df.loc[both_null_filtered.index, "artist"] = both_null_filtered["nominee"]
    
    # Extract artist from workers field when artist is missing
    def extract_artist(workers):
        if pd.isna(workers):
            return None
        match = re.search(r'\((.*?)\)', workers)
        return match.group(1) if match else None
    
    df["artist"] = df.apply(
        lambda row: extract_artist(row["workers"]) if pd.isna(row["artist"]) else row["artist"],
        axis=1
    )
    
    # Drop rows where artist is still missing
    df = df.dropna(subset=["artist"])
    
    # Drop workers column as we've extracted what we need
    if 'workers' in df.columns:
        df = df.drop(columns=["workers"])
    
    return df

def _normalize_artist_text(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize artist name formatting"""
    def normalize_text(text):
        if pd.isna(text):
            return text
        
        text = str(text)
        text = re.sub(r'\(([^)]*)\)', r'\1', text)  # Remove parentheses but keep content
        text = re.sub(r'\b(feat\.?|ft\.?|featuring|with|w/)\b', ';', text, flags=re.IGNORECASE)
        text = re.sub(r'\s&\s', ';', text)
        text = re.sub(r',\s', ';', text)
        text = text.replace('"', '')
        return text.strip()
    
    df['artist'] = df['artist'].apply(normalize_text)
    return df

def _extract_artists(df: pd.DataFrame) -> pd.DataFrame:
    """Extract primary and featured artists"""
    df['artist'] = df['artist'].astype(str)
    
    # Extract primary artist (first in list)
    df['primary_artist'] = df['artist'].str.split(';').str[0].str.strip()
    
    # Extract featured artists
    def extract_featured(artist_str):
        try:
            parts = artist_str.split(';')
            featured = [a.strip() for a in parts[1:] if a.strip()]
            return featured if featured else []
        except:
            return []
    
    df['featured_artists'] = df['artist'].apply(extract_featured)
    
    # Drop original artist column
    if 'artist' in df.columns:
        df = df.drop(columns=["artist"])
    
    return df

def _classify_award_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Classify award categories into broader groups"""
    def classify_category(category):
        if pd.isna(category):
            return 'other'
            
        category = category.lower()
        if 'album' in category:
            return 'album'
        elif any(kw in category for kw in ['song', 'record', 'performance']):
            return 'track'
        elif 'artist' in category:
            return 'artist'
        return 'other'
    
    df['award_class'] = df['category'].apply(classify_category)
    return df

def _create_artist_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Create summary statistics by artist"""
    # Combine primary and featured artists
    df['all_artists'] = df.apply(
        lambda row: [row['primary_artist']] + row['featured_artists'],
        axis=1
    )
    
    # Explode to create one row per artist
    exploded = df.explode('all_artists').copy()
    exploded = exploded.rename(columns={'all_artists': 'artist'})
    exploded['artist'] = exploded['artist'].astype(str)
    
    # Create summary statistics
    artist_grammy_nominations = (
        exploded.groupby('artist')
        .agg(
            grammy_nominations=('artist', 'count'),
            year_with_most_nominations=('year', lambda x: x.mode().iloc[0]),
            most_common_category=('category', lambda x: x.mode().iloc[0]),
            most_common_award_class=('award_class', lambda x: x.mode().iloc[0])
        )
        .reset_index()
    )
    
    return artist_grammy_nominations