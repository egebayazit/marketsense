# src/db/ops.py
from typing import Iterable, Dict, Any
from sqlalchemy import create_engine, text

# same SQLite URL we used before
ENGINE_URL = "sqlite:///data/marketsense.db"
engine = create_engine(ENGINE_URL, future=True)

def insert_stock_rows(rows: Iterable[Dict[str, Any]]) -> None:
    """
    rows: iterable of dicts with keys:
      ticker (str), date ('YYYY-MM-DD'), open (float), close (float), volume (int)
    Uses INSERT OR IGNORE so duplicates (by unique index) are skipped.
    """
    if not rows:
        return
    sql = text("""
        INSERT OR IGNORE INTO stocks (ticker, date, open, close, volume)
        VALUES (:ticker, :date, :open, :close, :volume)
    """)
    with engine.begin() as conn:
        conn.execute(sql, list(rows))

def insert_news_rows(rows: Iterable[Dict[str, Any]]) -> None:
    """
    rows: iterable of dicts with keys:
      headline (str), published_at ('YYYY-MM-DD HH:MM:SS' or ISO), source (str|None),
      url (str|None), ticker (str|None)
    Uses INSERT OR IGNORE; uniqueness is (headline, published_at).
    """
    if not rows:
        return
    sql = text("""
        INSERT OR IGNORE INTO news (headline, published_at, source, url, ticker)
        VALUES (:headline, :published_at, :source, :url, :ticker)
    """)
    with engine.begin() as conn:
        conn.execute(sql, list(rows))
