# scripts/debug_schema.py
import sqlite3

DB_PATH = "data/marketsense.db"

with sqlite3.connect(DB_PATH) as con:
    cur = con.cursor()
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='news'"
    ).fetchone()
    print("News table schema:\n", row[0])
