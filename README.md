# Lokalny Data Lakehouse (Demo)

Krótki projekt demonstracyjny pokazujący, jak zbudować nowoczesny, w pełni lokalny Data Lakehouse w architekturze **Medallion (Bronze - Silver - Gold)**, wykorzystując najszybsze dostępne narzędzia w ekosystemie Pythona.

Nie potrzebujesz klastra Sparka ani serwera bazy danych, wszystko działa błyskawicznie lokalnie!

## Stack Technologiczny

* **Polars** - ultraszybki, wielowątkowy silnik do przetwarzania danych (ETL) napisany w Rust.
* **Delta Table (`delta-rs`)** - format zapisu (ACID, Time Travel, logi transakcyjne) bez konieczności stawiania JVM/Sparka.
* **DuckDB** - analityczny silnik SQL działający w pamięci, natywnie odpytujący tabele Delta.

## Architektura Projektu

* `01_ingest.py` - **BRONZE** - Pobranie surowych plików Parquet ze strony NYC TLC (Taxi Dataset).
* `02_silver.py` - **SILVER** - Czyszczenie danych z użyciem `Polars` i zapis jako tabele `Delta`. Odporne na *Schema Drift*.
* `03_gold.py` - **GOLD** - Biznesowe agregacje (podsumowania miesięczne, top strefy).
* `04_query.py` - **ANALYTICS** - Błyskawiczne odpytywanie danych z użyciem `DuckDB` (język SQL).

## Jak uruchomić projekt?

```bash
git clone https://github.com/skublin/sfi-lakehouse

cd sfi-lakehouse

python -m venv .venv

source .venv/bin/activate  # na Windows: .venv\Scripts\activate

pip install --upgrade pip

pip install -r requirements.txt

chmod +x run_pipeline.sh

./run_pipeline.sh      # Linux / macOS, ewentualnie kolejno skrypty z src
```
