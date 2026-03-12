import time
import duckdb

def main():
    s = time.time()
    print("==== Rozpoczynam odpytywanie Lakehouse za pomocą DuckDB ====")
    
    # 1. Inicjalizacja połączenia w pamięci
    con = duckdb.connect()
    
    # 2. Instalacja i załadowanie oficjalnego rozszerzenia Delta dla DuckDB
    # Uwaga: Pobranie rozszerzenia wymaga połączenia z internetem przy pierwszym uruchomieniu
    print("[INFO] Ładowanie rozszerzenia 'delta'...")
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")
    
    # 3. Zapytanie do warstwy GOLD: Podsumowanie miesięczne
    print("\n--- Podsumowanie Miesięczne (Warstwa GOLD) ---")
    query_gold = """
        SELECT 
            month,
            total_trips,
            ROUND(total_fare_revenue, 2) AS total_revenue,
            ROUND(avg_distance_miles, 2) AS avg_distance_mi
        FROM delta_scan('data/gold/monthly_summary')
        ORDER BY month;
    """
    # Wykonanie zapytania i wyświetlenie sformatowanej tabeli w konsoli
    con.sql(query_gold).show()
    
    # 4. Zapytanie Ad-hoc do warstwy SILVER: Top 5 najbardziej dochodowych dni
    # DuckDB jest na tyle szybki, że możemy swobodnie odpytywać cięższą warstwę Silver
    print("\n--- Top 5 Najbardziej Dochodowych Dni (Zapytanie do warstwy SILVER) ---")
    query_silver = """
        SELECT 
            CAST(tpep_pickup_datetime AS DATE) AS trip_date,
            COUNT(*) AS total_trips,
            ROUND(SUM(fare_amount), 2) AS daily_revenue
        FROM delta_scan('data/silver/nyc_trips')
        GROUP BY trip_date
        ORDER BY daily_revenue DESC
        LIMIT 5;
    """
    con.sql(query_silver).show()
    
    print("==== Analiza zakończona sukcesem! ====")
    print(f"Czas wykonania: {time.time() - s:.2f}s.")

if __name__ == "__main__":
    main()
