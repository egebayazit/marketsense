# src/db/add_indexes.py
from sqlalchemy import create_engine, text

engine = create_engine("sqlite:///data/marketsense.db", future=True)

with engine.begin() as conn:
    # prevent duplicate (ticker, date) in stocks
    conn.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_stocks_ticker_date ON stocks(ticker, date)"
    ))

    # prevent duplicate news by (headline, published_at)
    conn.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_headline_published ON news(headline, published_at)"
    ))

print("✅ Unique indexes ensured.")
