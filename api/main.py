from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import os
import sqlite3

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# --- Config ---
DB_PATH = Path(os.getenv("DB_PATH", "data/marketsense.db")).resolve()

app = FastAPI(title="MarketSense API", version="0.5.0", docs_url="/docs")

# CORS (loose now; tighten for prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# --- Models ---
class StockClose(BaseModel):
    date: str = Field(..., description="ISO date (YYYY-MM-DD)")
    close: float

class NewsItem(BaseModel):
    published_at: str = Field(..., description="ISO datetime")
    source: str
    headline: str
    url: str

# --- DB helpers ---
def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"DB not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ticker_exists(conn: sqlite3.Connection, ticker: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stocks WHERE ticker = ? LIMIT 1;", (ticker.upper(),))
    return cur.fetchone() is not None

# --- Health ---
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

# =======================
# Stocks (2 endpoints)
# =======================
@app.get(
    "/stocks/{ticker}",
    response_model=List[StockClose],
    summary="Get last N calendar days of closing prices for a ticker",
)
def stocks_by_days(
    ticker: str,
    days: int = Query(7, ge=1, le=365, description="Calendar days to look back"),
):
    today_utc = datetime.now(timezone.utc).date()
    cutoff = (today_utc - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        if not ticker_exists(conn, ticker):
            raise HTTPException(status_code=400, detail=f"Unknown ticker: {ticker.upper()}")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date, close
            FROM stocks
            WHERE ticker = ?
              AND date >= ?
            ORDER BY date ASC
            """,
            (ticker.upper(), cutoff),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {ticker.upper()} in the last {days} day(s).")
    return [StockClose(date=str(r["date"])[:10], close=float(r["close"])) for r in rows]

@app.get(
    "/stocks/{ticker}/last-n",
    response_model=List[StockClose],
    summary="Get last N trading rows for a ticker",
)
def stocks_last_n(
    ticker: str,
    n: int = Query(7, ge=1, le=252, description="Number of trading rows to return"),
):
    with get_conn() as conn:
        if not ticker_exists(conn, ticker):
            raise HTTPException(status_code=400, detail=f"Unknown ticker: {ticker.upper()}")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date, close
            FROM stocks
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (ticker.upper(), n),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {ticker.upper()}.")
    # oldest → newest
    return [StockClose(date=str(r["date"])[:10], close=float(r["close"])) for r in reversed(rows)]

# =======================
# News (2 endpoints)
# =======================
@app.get(
    "/news",
    response_model=List[NewsItem],
    summary="Get recent news (calendar-day window with optional keyword)",
)
def news_by_days(
    days: int = Query(7, ge=1, le=60, description="Calendar days to look back"),
    q: Optional[str] = Query(None, description="Keyword (headline/source LIKE)"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    # midnight UTC cutoff so an index on published_at can be used
    today_utc = datetime.now(timezone.utc).date()
    cutoff_ts = f"{(today_utc - timedelta(days=days)).isoformat()}T00:00:00"

    sql = [
        "SELECT published_at, source, headline, url",
        "FROM news",
        "WHERE published_at >= ?",
    ]
    params: List[object] = [cutoff_ts]

    if q:
        sql.append("AND (headline LIKE ? OR source LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like])

    sql.append("ORDER BY published_at DESC")
    sql.append("LIMIT ? OFFSET ?")
    params.extend([limit, offset])

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("\n".join(sql), params)
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No news found for the given filters.")
    return [NewsItem(published_at=str(r["published_at"]), source=str(r["source"]), headline=str(r["headline"]), url=str(r["url"])) for r in rows]

@app.get(
    "/news/last-n",
    response_model=List[NewsItem],
    summary="Get latest N news rows (no date filter)",
)
def news_last_n(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT published_at, source, headline, url
            FROM news
            ORDER BY published_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No news available.")
    return [NewsItem(published_at=str(r["published_at"]), source=str(r["source"]), headline=str(r["headline"]), url=str(r["url"])) for r in rows]

# -----------------------
# Back-compat routes (hidden from docs)
# -----------------------
@app.get("/get-stock", include_in_schema=False)
def _compat_get_stock(ticker: str, days: int = 7):
    return stocks_by_days(ticker=ticker, days=days)

@app.get("/get-stock/last-n", include_in_schema=False)
def _compat_get_stock_last_n(ticker: str, n: int = 7):
    return stocks_last_n(ticker=ticker, n=n)

@app.get("/get-news", include_in_schema=False)
def _compat_get_news(days: int = 7, q: Optional[str] = None, limit: int = 20, offset: int = 0):
    return news_by_days(days=days, q=q, limit=limit, offset=offset)

@app.get("/news/latest", include_in_schema=False)
def _compat_news_latest(limit: int = 20, offset: int = 0):
    return news_last_n(limit=limit, offset=offset)
