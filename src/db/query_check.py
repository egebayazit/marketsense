# src/db/query_check.py
from sqlalchemy import create_engine, text

engine = create_engine("sqlite:///data/marketsense.db", future=True)

with engine.connect() as conn:
    print("=== Latest stocks ===")
    stocks = conn.execute(text("""
        SELECT ticker, date, close
        FROM stocks
        ORDER BY date DESC, ticker
        LIMIT 10;
    """)).fetchall()
    for r in stocks:
        print(r)

    print("\n=== Latest news ===")
    news = conn.execute(text("""
        SELECT headline, published_at, source
        FROM news
        ORDER BY id DESC
        LIMIT 5;
    """)).fetchall()
    for r in news:
        print(r)
