import time
import polars as pl
from pathlib import Path

# Definicja ścieżek
BRONZE_DIR = Path("data/bronze")
SILVER_TABLE_DIR = "data/silver/nyc_trips"

def main():
    s = time.time()
    print("==== Rozpoczynam przetwarzanie warstwy SILVER ====")
    
    Path(SILVER_TABLE_DIR).parent.mkdir(parents=True, exist_ok=True)
    print("[INFO] Budowanie planu zapytania (Lazy API) dla plików z Bronze...")
    
    # 1. ROZWIĄZANIE PROBLEMU SCHEMA DRIFT
    # Pobieramy listę wszystkich plików parquet
    parquet_files = list(BRONZE_DIR.glob("*.parquet"))
    
    lazy_dfs = []
    for file in parquet_files:
        # Skanujemy pojedynczy plik
        ldf = pl.scan_parquet(file).with_columns([
            # Wymuszamy spójny typ dla dat: mikrosekundy ('us')
            pl.col("tpep_pickup_datetime").cast(pl.Datetime("us")),
            pl.col("tpep_dropoff_datetime").cast(pl.Datetime("us"))
        ])
        lazy_dfs.append(ldf)
        
    # Łączymy wszystkie pliki w jedną wirtualną tabelę. 
    # Używamy how="diagonal_relaxed", aby Polars zignorował różnice, 
    # jeśli w jakimś miesiącu nagle pojawi się (lub zniknie) inna, mniej ważna kolumna.
    bronze_lazy_df = pl.concat(lazy_dfs, how="diagonal_relaxed")
    
    # 2. Czyszczenie i transformacja danych
    print("[INFO] Aplikowanie reguł czyszczenia (Data Quality)...")
    silver_lazy_df = (
        bronze_lazy_df
        .filter(
            (pl.col("trip_distance") > 0.0) & 
            (pl.col("fare_amount") >= 0.0)
        )
        .drop_nulls(subset=["PULocationID", "DOLocationID"])
        .with_columns(
            ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
             .dt.total_minutes()
             .alias("trip_duration_minutes"))
        )
        .filter(pl.col("trip_duration_minutes") > 0)
    )
    
    # 3. Wykonanie transformacji w pamięci
    print("[INFO] Uruchamianie obliczeń z użyciem silnika Polars (collect)...")
    silver_df = silver_lazy_df.collect()
    
    print(f"[INFO] Liczba poprawnych przejazdów po wyczyszczeniu: {silver_df.height:,}".replace(",", " "))
    
    # 4. Zapis do formatu Delta Lake
    print(f"[ZAPIS] Tworzenie tabeli Delta w katalogu: {SILVER_TABLE_DIR}")
    silver_df.write_delta(
        SILVER_TABLE_DIR,
        mode="overwrite",
        delta_write_options={"schema_mode": "overwrite"} 
    )
    
    print("==== Zakończono tworzenie warstwy SILVER! ====")
    print(f"Czas wykonania: {time.time() - s:.2f}s.")

if __name__ == "__main__":
    main()
