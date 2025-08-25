# scripts/jobs/fetch_prices.py
"""
Fetch recent daily prices for a small watchlist via yfinance and
idempotently upsert into SQLite `stocks` table.

Expected schema (see db/sqlite/001_init.sql):
  stocks(
    ticker TEXT NOT NULL,
    date   TEXT NOT NULL,           -- ISO YYYY-MM-DD
    open   REAL, high REAL, low REAL,
    close  REAL NOT NULL,
    volume INTEGER DEFAULT 0 CHECK (volume >= 0),
    PRIMARY KEY (ticker, date)
  )
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence
import logging
import re
import sqlite3

import pandas as pd
import yfinance as yf

# --------------------------------------------------------------------------------------
# Paths: always relative to repo root (…/scripts/jobs/fetch_prices.py -> parents[2])
# --------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "marketsense.db"
INIT_SQL = ROOT / "db" / "sqlite" / "001_init.sql"

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
TICKERS: list[str] = ["AAPL", "MSFT"]
DAYS: int = 10  # pull a small rolling window

# SQLite UPSERT (PK = (ticker, date))
UPSERT_SQL = """
INSERT INTO stocks (ticker, date, open, high, low, close, volume)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(ticker, date) DO UPDATE SET
  open   = excluded.open,
  high   = excluded.high,
  low    = excluded.low,
  close  = excluded.close,
  volume = excluded.volume
WHERE
  COALESCE(stocks.open,   -1.0) != COALESCE(excluded.open,   -1.0) OR
  COALESCE(stocks.high,   -1.0) != COALESCE(excluded.high,   -1.0) OR
  COALESCE(stocks.low,    -1.0) != COALESCE(excluded.low,    -1.0) OR
  COALESCE(stocks.close,  -1.0) != COALESCE(excluded.close,  -1.0) OR
  COALESCE(stocks.volume, -1   ) != COALESCE(excluded.volume, -1   );
"""

# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fetch_prices")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
_non_alnum = re.compile(r"[^a-z0-9]+")


def _norm(s: str) -> str:
    """lowercase and strip non-alphanumerics -> helps match yfinance weird col names"""
    return _non_alnum.sub("", s.lower())


def _first_match(
    columns: Sequence[str],
    want: Sequence[str],
    prefer_suffix: Optional[str] = None,
) -> Optional[str]:
    """
    Find first column whose normalized name starts with one of `want`.
    If `prefer_suffix` is provided (e.g., the ticker), prefer a column that ends with that.
    """
    norm_map = {c: _norm(c) for c in columns}
    # prefer “*_TICKER” if present
    if prefer_suffix:
        pref = prefer_suffix.lower()
        for c, n in norm_map.items():
            if any(n.startswith(_norm(w)) for w in want) and n.endswith(pref):
                return c
    # fallback: any matching start
    for c, n in norm_map.items():
        if any(n.startswith(_norm(w)) for w in want):
            return c
    return None


def ensure_schema(con: sqlite3.Connection) -> None:
    """Create tables if needed using INIT_SQL."""
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='stocks';"
    )
    exists = cur.fetchone() is not None
    if exists:
        return
    if INIT_SQL.exists():
        log.info("stocks table missing — applying schema from %s", INIT_SQL)
        with open(INIT_SQL, "r", encoding="utf-8") as f:
            con.executescript(f.read())
        log.info("Schema applied.")
    else:
        # minimalist create if init file is not present for any reason
        log.warning(
            "Init SQL not found at %s — creating minimal stocks table inline.",
            INIT_SQL,
        )
        con.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS stocks (
              ticker  TEXT NOT NULL,
              date    TEXT NOT NULL,
              open    REAL, high REAL, low REAL,
              close   REAL NOT NULL,
              volume  INTEGER DEFAULT 0 CHECK (volume >= 0),
              PRIMARY KEY (ticker, date)
            );
            """
        )


def fetch_daily_window(ticker: str, days: int = DAYS) -> pd.DataFrame:
    """
    Download recent daily OHLCV and normalize columns/types.
    Robust to MultiIndex/tuple columns and suffixed names like 'Close_AAPL'.
    """
    df = yf.download(
        ticker,
        period=f"{days}d",
        interval="1d",
        auto_adjust=False,   # be explicit (yfinance changed defaults)
        progress=False,
    )
    if df is None or df.empty:
        log.warning("No data returned for %s", ticker)
        return pd.DataFrame()

    # Flatten to simple columns
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex) or any(isinstance(c, tuple) for c in df.columns):
        flat_cols = []
        for c in df.columns:
            if isinstance(c, tuple):
                flat_cols.append("_".join([str(x) for x in c if x is not None]).strip())
            else:
                flat_cols.append(str(c))
        df.columns = flat_cols
    else:
        df.columns = [str(c) for c in df.columns]

    log.info("%s columns: %s", ticker, list(df.columns))

    # Resolve columns (handle e.g. Close_AAPL, Adj Close_AAPL)
    date_col = _first_match(df.columns, ["date", "datetime"]) or "Date"
    open_col = _first_match(df.columns, ["open"], ticker)
    high_col = _first_match(df.columns, ["high"], ticker)
    low_col = _first_match(df.columns, ["low"], ticker)
    # Prefer true Close over Adj Close if both exist
    close_col = (
        _first_match(df.columns, ["close"], ticker)
        or _first_match(df.columns, ["adj close", "adj_close"], ticker)
    )
    volume_col = _first_match(df.columns, ["volume"], ticker)

    if close_col is None:
        log.warning("No close/adj close column found for %s; skipping", ticker)
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d"),
            "open": df[open_col] if open_col in df.columns else None,
            "high": df[high_col] if high_col in df.columns else None,
            "low": df[low_col] if low_col in df.columns else None,
            "close": df[close_col],
            "volume": df[volume_col] if volume_col in df.columns else 0,
        }
    )
    out["ticker"] = ticker

    # Clean types / constraints
    out = out.dropna(subset=["close"])
    out["close"] = pd.to_numeric(out["close"], errors="coerce").fillna(0).clip(lower=0)

    for col in ["open", "high", "low"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            # keep NaNs as NULLs, otherwise clip negatives
            out[col] = out[col].where(out[col].isna(), out[col].clip(lower=0))

    out["volume"] = (
        pd.to_numeric(out["volume"], errors="coerce")
        .fillna(0)
        .astype("int64")
        .clip(lower=0)
    )

    # Ensure low <= high when both present
    if "low" in out.columns and "high" in out.columns:
        mask = out["low"].notna() & out["high"].notna() & (out["low"] > out["high"])
        if mask.any():
            swapped = int(mask.sum())
            low_vals = out.loc[mask, "low"].copy()
            out.loc[mask, "low"] = out.loc[mask, "high"].values
            out.loc[mask, "high"] = low_vals.values
            log.debug("Swapped %d rows to enforce low<=high for %s", swapped, ticker)

    log.info("Normalized frame for %s has %d rows", ticker, len(out))
    return out[["ticker", "date", "open", "high", "low", "close", "volume"]]


def upsert_df(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Bulk UPSERT rows; returns number of rows inserted/updated (SQLite rowcount)."""
    if df.empty:
        return 0
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    cur = con.cursor()
    cur.executemany(UPSERT_SQL, rows)
    return cur.rowcount or 0


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    log.info("Connecting to database at %s", DB_PATH)

    total = 0
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA foreign_keys = ON;")
        ensure_schema(con)

        for t in TICKERS:
            log.info("Fetching %d days for %s", DAYS, t)
            df = fetch_daily_window(t, days=DAYS)
            up = upsert_df(con, df)
            total += up
            log.info("%s: upserted/updated %d", t, up)

        con.commit()

    log.info("✅ Total upserted/updated rows: %d", total)


if __name__ == "__main__":
    main()
