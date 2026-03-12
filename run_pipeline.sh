#!/bin/bash

# Zatrzymanie skryptu w przypadku jakiegokolwiek błędu
set -e

echo "==================================================="
echo "START PIPELINE: NYC Taxi Local Lakehouse"
echo "==================================================="

echo ""
echo "[1/4] KROK: Uruchamianie warstwy BRONZE (Ingest)..."
python src/01_ingest.py

echo ""
echo "[2/4] KROK: Uruchamianie warstwy SILVER (Czyszczenie & Delta)..."
python src/02_silver.py

echo ""
echo "[3/4] KROK: Uruchamianie warstwy GOLD (Agregacje Biznesowe)..."
python src/03_gold.py

echo ""
echo "[4/4] KROK: Uruchamianie zapytań DUCKDB (Analytics)..."
python src/04_query.py

echo ""
echo "==================================================="
echo "PIPELINE ZAKOŃCZONY SUKCESEM!"
echo "==================================================="