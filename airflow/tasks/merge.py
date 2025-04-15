"""
Optimized Data Merging Module using rapidfuzz
"""

from rapidfuzz import process, fuzz
from tqdm import tqdm
import pandas as pd
from airflow.exceptions import AirflowException

def merge_datasets(**kwargs) -> pd.DataFrame:
    try:
        # Initialize tqdm for pandas
        tqdm.pandas()
        
        # Get DataFrames
        ti = kwargs['ti']
        spotify_df = ti.xcom_pull(task_ids='transform_phase.transform_spotify')
        grammy_df = ti.xcom_pull(task_ids='transform_phase.transform_grammy')
        lastfm_df = ti.xcom_pull(task_ids='transform_phase.transform_lastfm')
        
        # Validate
        if any(df is None or df.empty for df in [spotify_df, grammy_df, lastfm_df]):
            raise AirflowException("Missing input data")
            
        # Make copies
        df_spotify = spotify_df.copy()
        df_grammy = grammy_df.copy()
        df_lastfm = lastfm_df.copy()
        
        # Normalize names
        df_spotify['artist'] = df_spotify['artist'].str.strip().str.lower()
        df_grammy['artist'] = df_grammy['artist'].str.strip().str.lower()
        df_lastfm['artist'] = df_lastfm['artist'].str.strip().str.lower()
        
        # Get unique artists for matching
        spotify_artists = df_spotify['artist'].unique().tolist()
        
        # Optimized fuzzy matching with rapidfuzz
        def get_best_match(name, choices, threshold=90):
            result = process.extractOne(
                name, 
                choices, 
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold
            )
            return result[0] if result else None
        
        # Apply matching with progress bar
        df_grammy['matched_artist'] = df_grammy['artist'].progress_apply(
            lambda x: get_best_match(x, spotify_artists)
        )
        df_lastfm['matched_artist'] = df_lastfm['artist'].progress_apply(
            lambda x: get_best_match(x, spotify_artists)
        )
        
        # Prepare reduced datasets
        grammy_cols = ['matched_artist', 'grammy_nominations', 'year_with_most_nominations',
                      'most_common_category', 'most_common_award_class']
        lastfm_cols = ['matched_artist', 'lastfm_listeners', 'lastfm_playcount',
                      'similar_1', 'similar_2', 'similar_3']
        
        # Merge datasets
        df_merged = (df_spotify
            .merge(df_grammy[grammy_cols], 
                   how='left', 
                   left_on='artist', 
                   right_on='matched_artist')
            .merge(df_lastfm[lastfm_cols], 
                   how='left', 
                   left_on='artist', 
                   right_on='matched_artist',
                   suffixes=('', '_lastfm')))
        
        # Final processing
        df_merged['grammy_nominations'] = df_merged['grammy_nominations'].fillna(0).astype(int)
        df_merged = (df_merged
            .drop(columns=['matched_artist', 'matched_artist_lastfm'], errors='ignore')
            .drop_duplicates()
            .sort_values('grammy_nominations', ascending=False)
            .drop_duplicates(subset='artist', keep='first')
            .query("~(lastfm_listeners.isna() & lastfm_playcount.isna())"))
        
        return df_merged
        
    except Exception as e:
        raise AirflowException(f"Merge failed: {str(e)}")