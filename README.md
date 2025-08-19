# 🧠 MarketSense
AI-powered semantic financial news analyzer that uses vector embeddings, Qdrant, and intelligent agents to find and interpret market-moving news patterns.

## 📌 Project Vision
MarketSense allows users to:
- Ingest and analyze financial news  
- Find similar historical events using Qdrant vector search  
- Correlate headlines with market reactions  
- Query the system via AI agents for trading-relevant insights  

**Example:** “What happened to TSLA after similar recall headlines in the past?”

## 🧱 Tech Stack
- **FastAPI** – backend REST API  
- **Qdrant** – vector database  
- **sentence-transformers / FinBERT** – embedding generation  
- **LangChain** – agent orchestration  
- **yfinance** – stock price data (currently used)  
- *(Optional)* **NewsAPI** – news headlines (planned; current `news_collector.py` writes raw to `data/news_raw.json`)  
- **SQLite** – local relational DB for prices/news (dev)  
- **Streamlit** – optional UI dashboard  

## 🗃️ Database (SQLite)
Schema lives in `db/sqlite/001_init.sql`.

**Tables**
- **stocks** — daily bars:  
  Columns: `ticker TEXT`, `date TEXT (YYYY-MM-DD)`, `open REAL`, `high REAL`, `low REAL`, `close REAL NOT NULL`, `volume INTEGER`  
  - **Primary Key**: `(ticker, date)`  
  - **Constraints**: `volume >= 0`, `close >= 0`, `low <= high`  
  - **Indexes**: `idx_stocks_ticker_date_desc` on `(ticker, date DESC)` for fast “most recent” queries  
- **news** — headlines:  
  Columns: `id INTEGER PK`, `headline TEXT NOT NULL`, `published_at TEXT NOT NULL`, `source TEXT`, `url TEXT UNIQUE`  
  - **Indexes**: `idx_news_published_at` on `(published_at DESC)`

**Initialize DB (PowerShell)**
```powershell
python -c "import sqlite3, pathlib; sql=open(r'db/sqlite/001_init.sql','r',encoding='utf-8').read(); con=sqlite3.connect(r'data/marketsense.db'); con.executescript(sql); con.commit(); con.close(); print('DB initialized')"
