import requests

from pathlib import Path

# Konfiguracja
YEAR = "2024"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
TAXI_TYPE = "yellow"

# Oficjalny endpoint z danymi TLC (format Parquet)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Ścieżka docelowa dla warstwy Bronze
BRONZE_DIR = Path("data/bronze")

def download_file(url: str, dest_path: Path) -> None:
    """
    Pobiera plik z podanego URL i zapisuje go w dest_path.
    """
    if dest_path.exists():
        print(f"[INFO] Plik {dest_path.name} już istnieje. Pomijam pobieranie.")
        return

    print(f"[POBIERANIE] Rozpoczęto pobieranie: {url}")
    response = requests.get(url, stream=True)
    
    # Sprawdzenie, czy żądanie zakończyło się sukcesem (np. czy plik istnieje na serwerze)
    response.raise_for_status()

    # Zapisywanie pliku w kawałkach (chunkach), aby nie obciążać pamięci RAM
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    print(f"[SUKCES] Zapisano plik: {dest_path}")

def main():
    print("==== Zasilamy warstwę BRONZE ====")
    
    # Utworzenie katalogu docelowego, jeśli nie istnieje
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    for month in MONTHS:
        filename = f"{TAXI_TYPE}_tripdata_{YEAR}-{month}.parquet"
        url = f"{BASE_URL}/{filename}"
        dest_path = BRONZE_DIR / filename
        
        download_file(url, dest_path)

    print("==== Zakończono zasilanie danych! ====")

if __name__ == "__main__":
    main()
