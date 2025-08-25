# MarketSense (current phase: ingest-only)

This repo currently:
- Fetches daily OHLCV for a small watchlist via yfinance.
- Fetches financial news via NewsAPI.
- Stores both into a local SQLite DB at `data/marketsense.db`.
- Orchestrates the two steps daily with an Airflow DAG
  (`airflow/dags/marketsense_daily_ingest.py`) running inside
  `airflow_local` docker-compose.

## Run locally
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python src/db/db_setup.py
python scripts/jobs/fetch_prices.py
python scripts/jobs/fetch_news.py --mode daily
python scripts/dev/run_smoke_checks.py

## Run with Airflow (dev)
cd airflow_local
docker compose up -d
# open http://localhost:8080 (Admin/admin from .env)
# or test tasks:
docker compose exec scheduler \
  airflow tasks test marketsense_daily_ingest fetch_prices 2025-08-25
docker compose exec scheduler \
  airflow tasks test marketsense_daily_ingest fetch_news 2025-08-25

## Env
- Root `.env` for local runs (NEWS_API_KEY, etc.)
- `airflow_local/.env` for the containers (same keys)

## DB
Schema is from `db/sqlite/001_init.sql`.
