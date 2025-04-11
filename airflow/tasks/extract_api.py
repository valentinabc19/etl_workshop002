"""
Last.fm API Data Extraction Module
Handles artist data extraction from Last.fm API with checkpointing and error handling.
"""

import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Set, Optional
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LastFMExtractor:
    """Handles extraction of artist data from Last.fm API"""
    
    def __init__(self):
        self._setup_paths()
        self._load_config()
        self.api_key = self._load_api_key()
        self.base_delay = 0.5  # seconds between batches
        self.max_retries = 3
        self.batch_size = 10
        
    def _setup_paths(self) -> None:
        """Initialize all required paths and directories"""
        self.root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.artists_csv = os.path.join(self.root_dir, "data", "raw", "artists.csv")
        self.output_csv = os.path.join(self.root_dir, "data", "raw", "lastfm_data.csv")
        self.checkpoint_file = os.path.join(self.root_dir, "tmp", "lastfm_checkpoint.json")
        self.log_file = os.path.join(self.root_dir, "logs", "lastfm_errors.log")
        self.credentials_path = os.path.join(self.root_dir, "credentials.json")
        
        # Create directories if they don't exist
        os.makedirs(os.path.dirname(self.output_csv), exist_ok=True)
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        
    def _load_config(self) -> None:
        """Load configuration parameters"""
        self.api_url = "http://ws.audioscrobbler.com/2.0/"
        self.request_timeout = 10  # seconds
        
    def _load_api_key(self) -> str:
        """Load and validate API key from credentials file"""
        try:
            with open(self.credentials_path, "r", encoding="utf-8") as file:
                credentials = json.load(file)
            
            if not (api_key := credentials.get("api_key")):
                raise ValueError("API key not found in credentials.json")
                
            return api_key
        except Exception as e:
            self._log_error(f"Error loading API key: {str(e)}")
            raise RuntimeError(f"API key loading failed: {str(e)}")
    
    def _log_error(self, message: str) -> None:
        """Log errors with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp}: {message}\n"
        
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_entry)
        except Exception as e:
            logger.error(f"Failed to write to log file: {str(e)}")
    
    def load_artists(self) -> List[str]:
        """Load artist names from CSV file"""
        try:
            if not os.path.exists(self.artists_csv):
                raise FileNotFoundError(f"Artists CSV not found at {self.artists_csv}")
                
            df = pd.read_csv(self.artists_csv)
            if 'primary_artist' not in df.columns:
                raise ValueError("CSV missing required 'primary_artist' column")
                
            return df['primary_artist'].dropna().unique().tolist()
        except Exception as e:
            self._log_error(f"Error loading artists: {str(e)}")
            raise
    
    def _load_checkpoint(self) -> Dict[str, List[str]]:
        """Load processing checkpoint"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self._log_error(f"Error loading checkpoint: {str(e)}")
                return {"processed": [], "failed": []}
        return {"processed": [], "failed": []}
    
    def _save_checkpoint(self, processed: List[str], failed: List[str]) -> None:
        """Save processing progress"""
        try:
            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({"processed": processed, "failed": failed}, f)
        except Exception as e:
            self._log_error(f"Error saving checkpoint: {str(e)}")
            raise
    
    def _make_api_request(self, artist: str, retry_count: int = 0) -> Optional[Dict]:
        """Make API request with retry logic"""
        params = {
            "method": "artist.getInfo",
            "artist": artist,
            "api_key": self.api_key,
            "format": "json",
            "autocorrect": 1
        }
        
        try:
            response = requests.get(
                self.api_url,
                params=params,
                timeout=self.request_timeout
            )
            
            if response.status_code == 200:
                return response.json().get('artist')
                
            elif response.status_code == 429:  # Rate limited
                retry_after = int(response.headers.get('Retry-After', 5))
                wait_time = max(retry_after, (2 ** retry_count))
                logger.warning(f"Rate limit hit for {artist}. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                
                if retry_count < self.max_retries:
                    return self._make_api_request(artist, retry_count + 1)
                return None
                
            else:
                self._log_error(f"HTTP {response.status_code} for {artist}")
                return None
                
        except requests.exceptions.RequestException as e:
            self._log_error(f"Request failed for {artist}: {str(e)}")
            return None
    
    def _transform_artist_data(self, artist: str, api_data: Dict) -> Dict:
        """Transform raw API response to structured format"""
        return {
            "artist_name": artist,
            "listeners": api_data.get('stats', {}).get('listeners', 0),
            "playcount": api_data.get('stats', {}).get('playcount', 0),
            "similar_artists": ";".join(
                artist['name'] 
                for artist in api_data.get('similar', {}).get('artist', [])[:3]
            ),
            "tags": ";".join(
                tag['name'] 
                for tag in api_data.get('tags', {}).get('tag', [])[:5]
            ),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def _load_existing_results(self) -> List[Dict]:
        """Load previously saved results"""
        if os.path.exists(self.output_csv):
            try:
                return pd.read_csv(self.output_csv).to_dict('records')
            except Exception as e:
                self._log_error(f"Error loading existing results: {str(e)}")
                return []
        return []
    
    def extract_artist_data(self) -> None:
        """Main extraction workflow"""
        try:
            artists = self.load_artists()
            if not artists:
                logger.warning("No artists found to process")
                return

            checkpoint = self._load_checkpoint()
            processed = set(checkpoint["processed"])
            failed = set(checkpoint["failed"])
            
            remaining = [
                a for a in artists 
                if a not in processed and a not in failed
            ]
            
            if not remaining:
                logger.info("All artists already processed")
                return

            logger.info(
                f"Artists remaining: {len(remaining)}/{len(artists)} | "
                f"Previously processed: {len(processed)} | Failed: {len(failed)}"
            )

            results = self._load_existing_results()
            
            with tqdm(total=len(remaining), desc="Processing artists") as pbar:
                for i in range(0, len(remaining), self.batch_size):
                    batch = remaining[i:i + self.batch_size]
                    batch_results = []
                    
                    for artist in batch:
                        if data := self._make_api_request(artist):
                            batch_results.append(
                                self._transform_artist_data(artist, data)
                            )
                            processed.add(artist)
                        else:
                            failed.add(artist)
                        
                        pbar.update(1)
                    
                    # Update checkpoint and save results
                    self._save_checkpoint(list(processed), list(failed))
                    results.extend(batch_results)
                    
                    try:
                        pd.DataFrame(results).to_csv(
                            self.output_csv, 
                            index=False,
                            encoding='utf-8'
                        )
                    except Exception as e:
                        self._log_error(f"Error saving results: {str(e)}")
                        raise
                    
                    # Rate limiting delay
                    if i + self.batch_size < len(remaining):
                        time.sleep(self.base_delay)

            # Clean up checkpoint if completed
            if os.path.exists(self.checkpoint_file):
                try:
                    os.remove(self.checkpoint_file)
                except Exception as e:
                    self._log_error(f"Error removing checkpoint: {str(e)}")

            logger.info(f"Completed. Data saved to {self.output_csv}")

        except Exception as e:
            self._log_error(f"Extraction failed: {str(e)}")
            raise RuntimeError(f"Extraction failed: {str(e)}") from e


def extract_lastfm_data():
    """Entry point for Airflow DAG"""
    extractor = LastFMExtractor()
    extractor.extract_artist_data()


if __name__ == "__main__":
    extract_lastfm_data()