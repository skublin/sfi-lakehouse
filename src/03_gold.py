import time
import polars as pl
from pathlib import Path

# Definicja ścieżek
SILVER_TABLE_DIR = "data/silver/nyc_trips"
GOLD_DIR = Path("data/gold")

def main():
    s = time.time()
    print("==== Rozpoczynam budowę warstwy GOLD ====")
    
    # Upewniamy się, że główny katalog Gold istnieje
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Wczytanie tabeli Delta (Silver) za pomocą Lazy API
    print(f"[INFO] Odczyt tabeli Delta z: {SILVER_TABLE_DIR}...")
    # pl.scan_delta pozwala na odczyt z zachowaniem optymalizacji zapytań
    silver_df = pl.scan_delta(SILVER_TABLE_DIR)
    
    # 2. Agregacja biznesowa nr 1: Podsumowanie miesięczne
    print("[INFO] Obliczanie: Podsumowanie miesięczne (przychodów i przejazdów)...")
    monthly_summary = (
        silver_df
        # Ekstrakcja samego miesiąca z pełnej daty
        .with_columns(pl.col("tpep_pickup_datetime").dt.month().alias("month"))
        .group_by("month")
        .agg([
            pl.len().alias("total_trips"),
            pl.col("fare_amount").sum().alias("total_fare_revenue"),
            pl.col("tip_amount").sum().alias("total_tips"),
            pl.col("trip_distance").mean().alias("avg_distance_miles"),
            pl.col("trip_duration_minutes").mean().alias("avg_duration_minutes")
        ])
        .sort("month")
        .collect() # Wykonanie obliczeń
    )
    
    # 3. Agregacja biznesowa nr 2: Top 20 stref (Pickup Zones)
    print("[INFO] Obliczanie: Najpopularniejsze strefy startowe...")
    top_zones = (
        silver_df
        .group_by("PULocationID")
        .agg([
            pl.len().alias("total_trips"),
            pl.col("fare_amount").mean().alias("avg_fare"),
            pl.col("tip_amount").mean().alias("avg_tip")
        ])
        # Sortujemy malejąco po liczbie przejazdów i bierzemy top 20
        .sort("total_trips", descending=True)
        .limit(20)
        .collect() # Wykonanie obliczeń
    )
    
    # 4. Zapis wyników do nowych tabel Delta
    print("[ZAPIS] Zapisywanie zagregowanych tabel Delta do warstwy Gold...")
    monthly_summary.write_delta(str(GOLD_DIR / "monthly_summary"), mode="overwrite")
    top_zones.write_delta(str(GOLD_DIR / "top_zones"), mode="overwrite")
    
    print("==== Zakończono tworzenie warstwy GOLD! ====")
    print(f"Czas wykonania: {time.time() - s:.2f}s.")

if __name__ == "__main__":
    main()
