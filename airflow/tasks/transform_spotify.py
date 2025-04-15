"""
Spotify Data Transformation Module for Airflow DAG
Handles cleaning and transformation of Spotify streaming data
Returns the fully transformed DataFrame with all modifications applied
"""

import pandas as pd
from airflow.exceptions import AirflowException

def transform_spotify_data(**kwargs) -> pd.DataFrame:
    """
    Main transformation function for Spotify data in Airflow DAG
    
    Args:
        kwargs: Airflow context
        
    Returns:
        pd.DataFrame: Fully transformed Spotify data with all modifications applied
        
    Raises:
        AirflowException: If transformation fails at any step
    """
    try:
        # Get DataFrame from previous task via XCom
        ti = kwargs['ti']
        spotify_df = ti.xcom_pull(task_ids='extract_phase.extract_spotify')
        
        if spotify_df is None or spotify_df.empty:
            raise AirflowException("No data received from extraction task")
            
        # Make a copy to avoid modifying the original
        spotify_df = spotify_df.copy()
        
        # Step 1: Initial column cleanup
        spotify_df = _clean_unnecessary_columns(spotify_df)
        
        # Step 2: Remove duplicates
        spotify_df = _remove_duplicates(spotify_df)
        
        # Step 3: Clean text fields
        spotify_df = _clean_text_fields(spotify_df)
        
        # Step 4: Extract primary artist and features
        spotify_df = _extract_artist_info(spotify_df)
        
        # Step 5: Convert duration and clean data
        spotify_df = _convert_duration(spotify_df)
        
        # Step 6: Categorize audio features
        spotify_df = _categorize_features(spotify_df)

        spotify_df = _create_final_df(spotify_df)
        
        return spotify_df
        
    except Exception as e:
        raise AirflowException(f"Spotify data transformation failed: {str(e)}")

def _clean_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove unnecessary columns from the DataFrame"""
    columns_to_drop = ["Unnamed: 0", "album_name", "key", "mode", 
                      "time_signature", "tempo"]
    return df.drop(columns=[col for col in columns_to_drop if col in df.columns])

def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate tracks from the DataFrame"""
    df = df.drop_duplicates()
    return df.drop_duplicates(subset='track_id')

def _clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize text fields (artists and track names)"""
    text_columns = ['artists', 'track_name']
    
    for col in text_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .str.strip()  
                .str.replace(r'[\"]', '', regex=True) 
                .str.replace(r'\s+', ' ', regex=True) 
            )
    
    return df

def _extract_artist_info(df: pd.DataFrame) -> pd.DataFrame:
    """Extract primary artist and feature information"""
    if 'artists' in df.columns:
        df['primary_artist'] = df['artists'].str.split(';').str[0].str.strip()
        df['is_feature'] = df['artists'].str.contains(';').astype('bool')
        df = df[df['artists'].notna()]
    return df

def _convert_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Convert duration from ms to minutes and drop original column"""
    if 'duration_ms' in df.columns:
        df['duration_min'] = round(df['duration_ms'] / (1000 * 60), 2)
        df = df.drop(columns=['duration_ms'])
    return df

def _categorize_features(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize audio features into meaningful groups"""
    if not all(col in df.columns for col in ['popularity', 'danceability', 'energy']):
        return df
    
    # Duration categories
    if 'duration_min' in df.columns:
        df['duration_cat'] = pd.cut(
            df['duration_min'],
            bins=[0, 3, 4, float('inf')],
            labels=['Short (<3 min)', 'Medium (3-4 min)', 'Long (>4 min)']
        )
    
    # Popularity categories
    df['popularity_cat'] = pd.cut(
        df['popularity'],
        bins=[-1, 40, 70, 100],
        labels=['Low', 'Medium', 'High']
    )
    
    # Audio feature categories
    feature_config = {
        'danceability': ([-0.01, 0.4, 0.7, 1.0], ['Low', 'Medium', 'High']),
        'energy': ([-0.01, 0.4, 0.7, 1.0], ['Low', 'Medium', 'High']),
        'loudness': ([-60, -20, -10, 0], ['Quiet', 'Moderate', 'Loud']),
        'speechiness': ([-0.01, 0.33, 0.66, 1.0], ['Low', 'Moderate', 'High']),
        'acousticness': ([-0.01, 0.3, 0.7, 1.0], ['Low', 'Moderate', 'High']),
        'instrumentalness': ([-0.01, 0.3, 0.7, 1.0], ['Vocal', 'Mixed', 'Instrumental']),
        'liveness': ([-0.01, 0.3, 0.7, 1.0], ['Studio', 'Hybrid', 'Live']),
        'valence': ([-0.01, 0.3, 0.7, 1.0], ['Sad', 'Neutral', 'Happy'])
    }
    
    for feature, (bins, labels) in feature_config.items():
        if feature in df.columns:
            df[feature] = pd.cut(df[feature], bins=bins, labels=labels)
    
    return df

def _create_final_df(df: pd.DataFrame) -> pd.DataFrame:
    """Create final dataframe"""
    if 'primary_artist' not in df.columns:
        return pd.DataFrame()
    
    spotify_final_df = df.groupby('primary_artist', as_index=False).agg({
        'track_name': 'count', 
        'popularity': 'mean',
        'danceability': lambda x: x.mode()[0] if not x.mode().empty else None,
        'energy': lambda x: x.mode()[0] if not x.mode().empty else None,
        'speechiness': lambda x: x.mode()[0] if not x.mode().empty else None,
        'instrumentalness': lambda x: x.mode()[0] if not x.mode().empty else None,
        'explicit': lambda x: (x.sum() / len(x)) * 100,
        'duration_min': 'mean',
        'is_feature': lambda x: (x.sum() / len(x)) * 100,
        'track_genre': lambda x: x.mode()[0] if not x.mode().empty else None
    }).rename(columns={
        'primary_artist': 'artist',
        'track_name': 'total_tracks',
        'popularity': 'spotify_popularity',
        'explicit': 'pct_explicit_tracks',
        'is_feature': 'feature_pct',
        'track_genre': 'spotify_track_genre'
    })
    
    return spotify_final_df