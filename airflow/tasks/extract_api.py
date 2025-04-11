"""
Last.fm Data Extractor for Airflow DAGs
Maintains original functionality while being Airflow-compatible
"""

import requests
import time
import pandas as pd
import os
import json
from tqdm import tqdm
from datetime import datetime
from airflow.exceptions import AirflowException

# Constants (same as original)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTISTS_CSV = os.path.join(ROOT_DIR, "data", "raw", "artists.csv")
OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "raw", "lastfm_data.csv")
CHECKPOINT_FILE = os.path.join(ROOT_DIR, "tmp", "lastfm_checkpoint.json")
LOG_FILE = os.path.join(ROOT_DIR, "logs", "lastfm_errors.log")
CREDENTIALS_PATH = os.path.join(ROOT_DIR, "credentials.json")

# Configuration (same as original)
BATCH_SIZE = 10
BASE_DELAY = 0.5
MAX_RETRIES = 3

def load_api_key():
    """Load API key from credentials file (same as original)"""
    try:
        with open(CREDENTIALS_PATH, "r", encoding="utf-8") as file:
            credentials = json.load(file)
        api_key = credentials.get("api_key")
        if not api_key:
            raise ValueError("API key not found in credentials.json")
        return api_key
    except Exception as e:
        log_error(f"Error loading API key: {str(e)}")
        raise AirflowException(f"API key loading failed: {str(e)}")

def load_artists():
    """Load artists from CSV (same as original)"""
    try:
        df = pd.read_csv(ARTISTS_CSV)
        return df['primary_artist'].tolist()  
    except Exception as e:
        log_error(f"Error loading artists: {str(e)}")
        raise AirflowException(f"Artist loading failed: {str(e)}")

def load_checkpoint():
    """Load checkpoint (same as original)"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"processed": [], "failed": []}
    return {"processed": [], "failed": []}

def save_checkpoint(processed, failed):
    """Save checkpoint (same as original)"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"processed": processed, "failed": failed}, f)

def log_error(message):
    """Error logging (same as original)"""
    with open(LOG_FILE, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

def get_artist_data(artist, api_key, retry_count=0):      
    """Get artist data from API (same as original but with api_key parameter)"""
    params = {
        "method": "artist.getInfo",
        "artist": artist,
        "api_key": api_key,
        "format": "json",
        "autocorrect": 1
    }
    
    try:
        response = requests.get(
            "http://ws.audioscrobbler.com/2.0/",
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            return response.json().get('artist', None)
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            wait_time = max(retry_after, (2 ** retry_count))
            print(f"Rate limit reached. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            return get_artist_data(artist, api_key, retry_count + 1) if retry_count < MAX_RETRIES else None
        else:
            log_error(f"HTTP {response.status_code} for {artist}")
            return None
            
    except Exception as e:
        log_error(f"Exception for {artist}: {str(e)}")
        return None

def extract_lastfm_data(**kwargs) -> pd.DataFrame:
    """
    Main extraction function adapted for Airflow
    Preserves all original functionality while being Airflow-compatible
    Returns:
        pd.DataFrame: Data loaded from the generated CSV file
    """
    # Ensure directories exist (same as original)
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # Load API key
    api_key = load_api_key()
    
    # Load artists
    try:
        artists = load_artists()
        if not artists:
            print("No artists found to process")
            return pd.DataFrame()
    except Exception as e:
        raise AirflowException(f"Artist loading failed: {str(e)}")

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed = set(checkpoint["processed"])
    failed = set(checkpoint["failed"])
    
    remaining = [a for a in artists if a not in processed and a not in failed]
    total_artists = len(remaining)
    
    if not remaining:
        print("All artists already processed")
        # Return existing data if available
        if os.path.exists(OUTPUT_CSV):
            return pd.read_csv(OUTPUT_CSV)
        return pd.DataFrame()

    print(f"\nArtists remaining: {total_artists}/{len(artists)}")
    print(f"Previously processed: {len(processed)} | Failed: {len(failed)}")

    # Load existing results
    results = []
    if os.path.exists(OUTPUT_CSV):
        try:
            results = pd.read_csv(OUTPUT_CSV).to_dict('records')
        except Exception as e:
            log_error(f"Error loading existing results: {str(e)}")
            results = []

    # Process artists
    try:
        with tqdm(total=total_artists, desc="Processing artists") as pbar:
            for i in range(0, total_artists, BATCH_SIZE):
                batch = remaining[i:i + BATCH_SIZE]
                batch_results = []
                
                for artist in batch:
                    data = get_artist_data(artist, api_key)
                    if data:
                        batch_results.append({
                            "artist_name": artist,
                            "listeners": data.get('stats', {}).get('listeners', 0),
                            "playcount": data.get('stats', {}).get('playcount', 0),
                            "similar": ";".join([s['name'] for s in data.get('similar', {}).get('artist', [])][:3])
                        })
                        processed.add(artist)
                    else:
                        failed.add(artist)
                    
                    save_checkpoint(list(processed), list(failed))
                    pbar.update(1)
                
                results.extend(batch_results)
                
                # Save intermediate results
                try:
                    pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
                except Exception as e:
                    log_error(f"Error saving results: {str(e)}")
                    raise AirflowException(f"Failed to save results: {str(e)}")
                
                # Rate limiting
                if i + BATCH_SIZE < total_artists:
                    time.sleep(BASE_DELAY)

        # Clean up checkpoint if completed
        if os.path.exists(CHECKPOINT_FILE):
            try:
                os.remove(CHECKPOINT_FILE)
            except Exception as e:
                log_error(f"Error removing checkpoint: {str(e)}")

        print(f"\nProcess completed. Data saved to: {OUTPUT_CSV}")
        
        return pd.read_csv(OUTPUT_CSV)

    except Exception as e:
        log_error(f"Extraction failed: {str(e)}")
        raise AirflowException(f"Extraction failed: {str(e)}")

# Maintain original standalone functionality
if __name__ == "__main__":
    print("=== Last.fm Data Scraper ===")
    print(f"Root directory: {ROOT_DIR}")
    extract_lastfm_data()