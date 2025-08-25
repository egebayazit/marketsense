# tests/test_smoke_news.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data/marketsense.db")

def test_no_duplicate_news_url():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total = cur.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    distinct_urls = cur.execute("SELECT COUNT(DISTINCT url) FROM news").fetchone()[0]
    con.close()
    assert total == distinct_urls, f"Duplicate URLs detected: total={total} distinct={distinct_urls}"
