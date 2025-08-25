# MarketSense – Progress Log

This file tracks development milestones, experiments, and daily progress.
The main `README.md` remains focused on setup + long-term goals.

---

## ✅ Current Progress (as of Aug 25, 2025)

We now have a working **daily ingestion pipeline** powered by **Apache Airflow**:

- **Scripts:**
  - `scripts/jobs/fetch_prices.py` → pulls daily OHLCV data (AAPL, MSFT) from Yahoo Finance and upserts into SQLite (`stocks` table).
  - `scripts/jobs/fetch_news.py` → fetches financial headlines from NewsAPI and upserts into SQLite (`news` table).
- **Database:**
  - SQLite DB at `data/marketsense.db`
  - Tables: `stocks`, `news` (both idempotent upserts, safe to re-run).
- **Airflow DAG:**
  - `airflow/dags/marketsense_daily_ingest.py` runs both jobs in sequence.
  - Confirmed to succeed: DAG runs insert new rows daily without duplicates.
- **Verification:**
  - `scripts/dev/check_db.py` prints current DB schema and row counts.
  - Manual test: clearing DB → running DAG → verified fresh ingestion of ~20 stock bars + ~100 news headlines.

---

## 🗓️ Daily Log

### Aug 25, 2025
- Fixed SQLite schema initialization (`001_init.sql` applied).
- Enhanced `fetch_prices.py` with logging + robust column normalization (works locally & in Airflow).
- Enhanced `fetch_news.py` with logging + idempotent NewsAPI ingestion.
- Verified Airflow DAG ingestion → `stocks` (20 rows), `news` (97 rows).
- Tested DB clearing + rerun: ingestion reproducible & idempotent.

---

## 💡 Next Focus
- Automate daily scheduling via Airflow (cron).
- Decide on local vs remote always-on deployment.
- Start embedding + Qdrant integration.
