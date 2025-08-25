import sqlite3
from pathlib import Path

DB_PATH = Path("data/marketsense.db")

def test_no_duplicate_ticker_date():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total = cur.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    distinct_pairs = cur.execute("""
        SELECT COUNT(*) FROM (SELECT DISTINCT ticker, date FROM stocks)
    """).fetchone()[0]
    con.close()
    assert total == distinct_pairs, f"Found duplicates: total={total} distinct={distinct_pairs}"
