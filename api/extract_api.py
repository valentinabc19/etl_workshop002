import requests
import time
import pandas as pd
import os
import json
from tqdm import tqdm
from datetime import datetime

# --- Configuración de rutas ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTISTS_CSV = os.path.join(ROOT_DIR, "data", "raw", "artists.csv")
OUTPUT_CSV = os.path.join(ROOT_DIR, "data", "raw", "lastfm_data.csv")
CHECKPOINT_FILE = os.path.join(ROOT_DIR, "tmp", "lastfm_checkpoint.json")
LOG_FILE = os.path.join(ROOT_DIR, "logs", "lastfm_errors.log")

# Crear carpetas si no existen
os.makedirs(os.path.dirname(ARTISTS_CSV), exist_ok=True)
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# --- Configuración API ---
API_KEY = "f734d9017ce700181dfbfd9282257ac5"  # Reemplaza con tu API key
BATCH_SIZE = 10
BASE_DELAY = 0.5
MAX_RETRIES = 3

# --- Cargar artistas ---
def load_artists():
    """Carga artistas desde CSV con manejo de errores"""
    try:
        df = pd.read_csv(ARTISTS_CSV)
        return df['primary_artist'].tolist()  
    except Exception as e:
        print(f"Error cargando artistas: {str(e)}")
        return []

# --- Persistencia ---
def load_checkpoint():
    """Carga el progreso desde JSON"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"processed": [], "failed": []}
    return {"processed": [], "failed": []}

def save_checkpoint(processed, failed):
    """Guarda progreso en JSON"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"processed": processed, "failed": failed}, f)

def log_error(message):
    """Log de errores con timestamp"""
    with open(LOG_FILE, 'a') as f:
        f.write(f"{datetime.now()}: {message}\n")

# --- API Request ---
def get_artist_data(artist, retry_count=0):
    params = {
        "method": "artist.getInfo",
        "artist": artist,
        "api_key": API_KEY,
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
            print(f"Rate limit alcanzado. Esperando {wait_time} segundos...")
            time.sleep(wait_time)
            return get_artist_data(artist, retry_count + 1) if retry_count < MAX_RETRIES else None
        else:
            log_error(f"HTTP {response.status_code} para {artist}")
            return None
            
    except Exception as e:
        log_error(f"Exception en {artist}: {str(e)}")
        return None

# --- Procesamiento principal ---
def fetch_artist_data():
    artists = load_artists()
    if not artists:
        print("No se encontraron artistas para procesar")
        return

    checkpoint = load_checkpoint()
    processed = set(checkpoint["processed"])
    failed = set(checkpoint["failed"])
    
    remaining = [a for a in artists if a not in processed and a not in failed]
    total_artists = len(remaining)
    
    if not remaining:
        print("Todos los artistas ya fueron procesados")
        return

    print(f"\nArtistas pendientes: {total_artists}/{len(artists)}")
    print(f"Previamente procesados: {len(processed)} | Fallidos: {len(failed)}")

    results = []
    if os.path.exists(OUTPUT_CSV):
        results = pd.read_csv(OUTPUT_CSV).to_dict('records')

    with tqdm(total=total_artists, desc="Procesando artistas") as pbar:
        for i in range(0, total_artists, BATCH_SIZE):
            batch = remaining[i:i + BATCH_SIZE]
            batch_results = []
            
            for artist in batch:
                data = get_artist_data(artist)
                if data:
                    batch_results.append({
                        "artist_name": artist,
                        "listeners": data.get('stats', {}).get('listeners', 0),
                        "playcount": data.get('stats', {}).get('playcount', 0),
                        "tags": ";".join([t['name'] for t in data.get('tags', {}).get('tag', [])][:5]),
                        "similar": ";".join([s['name'] for s in data.get('similar', {}).get('artist', [])][:3])
                    })
                    processed.add(artist)
                else:
                    failed.add(artist)
                
                # Guardar progreso después de cada artista
                save_checkpoint(list(processed), list(failed))
                pbar.update(1)
            
            results.extend(batch_results)
            
            # Guardar resultados parciales
            pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
            
            if i + BATCH_SIZE < total_artists:
                time.sleep(BASE_DELAY)

    # Limpieza final
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print(f"\nProceso completado. Datos guardados en: {OUTPUT_CSV}")

# --- Ejecución ---
if __name__ == "__main__":
    print("=== Last.fm Data Scraper ===")
    print(f"Root directory: {ROOT_DIR}")
    fetch_artist_data()