# src/db/queries.py
from typing import List, Tuple
from sqlalchemy import create_engine, text
import os

# Configurable DB URL; falls back to local SQLite file
DB_URL = os.getenv("MARKETSENSE_DB_URL", "sqlite:///data/marketsense.db")
engine = create_engine(DB_URL, future=True)

def get_recent_closes(ticker: str, days: int = 7) -> List[Tuple[str, float]]:
    n = int(days)
    sql = text(f"""
        SELECT date, close
        FROM stocks
        WHERE ticker = :t
        ORDER BY date DESC
        LIMIT {n};
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"t": ticker.upper()}).fetchall()
    # Return oldest→newest for charts
    return list(reversed(rows))

def get_latest_news(limit: int = 20) -> List[Tuple[str, str, str]]:
    l = int(limit)
    sql = text(f"""
        SELECT headline, published_at, COALESCE(source, 'unknown') as source
        FROM news
        ORDER BY id DESC
        LIMIT {l};
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return rows
