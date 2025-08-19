import sqlite3
import pathlib
import sys

DB = pathlib.Path("data/marketsense.db")
SQL_PATH = pathlib.Path("tests/sql/smoke_checks.sql")

def run():
    if not DB.exists():
        print(f"DB not found: {DB}")
        sys.exit(1)
    if not SQL_PATH.exists():
        print(f"SQL not found: {SQL_PATH}")
        sys.exit(1)

    con = sqlite3.connect(DB)
    cur = con.cursor()
    print("-- Running smoke checks --")
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
        try:
            cur.execute(stmt)
            rows = cur.fetchall()
            if rows:
                print(f"\nResult for:\n{stmt[:80]}...")
                for r in rows:
                    print(r)
            else:
                print(f"\nOK (no rows):\n{stmt[:80]}...")
        except Exception as e:
            print(f"\nERROR on:\n{stmt[:80]}...\n{e}")
    con.close()

if __name__ == "__main__":
    run()
