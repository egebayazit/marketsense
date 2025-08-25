# scripts/reset_news_table.py

import sqlite3
from pathlib import Path

DB_PATH = Path("data/marketsense.db")

NEWS_SCHEMA = """
DROP TABLE IF EXISTS news;

CREATE TABLE news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    headline     TEXT NOT NULL,
    published_at TEXT NOT NULL,   -- stored as ISO string (YYYY-MM-DDTHH:MM:SS)
    source       TEXT,
    url          TEXT UNIQUE      -- ✅ ensures no duplicate articles
);
"""

def main():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.executescript(NEWS_SCHEMA)
        con.commit()
        print("✅ news table reset with url UNIQUE")

if __name__ == "__main__":
    main()
